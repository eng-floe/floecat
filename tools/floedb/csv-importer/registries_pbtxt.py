#!/usr/bin/env python3
#
# Copyright 2026 Yellowbrick Data, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Generate FloeCat PBtxt for registry-level index dictionaries.

Emits a single registries.pbtxt containing registry-level EngineSpecific payloads:

- floe.access_methods+proto          (pg_am)
- floe.operator_families+proto       (pg_opfamily)
- floe.operator_classes+proto        (pg_opclass)
- floe.operator_access_methods+proto     (pg_amop)
- floe.procedure_access_methods+proto      (pg_amproc)

All entries are filtered to system objects only (OIDs < 16384) where applicable.

NOTE:
These payloads are meant to be attached to SystemObjectsRegistry.engine_specific.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from utils import (
    AmRow,
    AmopRow,
    AmprocRow,
    OpclassRow,
    OpfamilyRow,
    build_proc_name_to_oid_map,
    is_system_oid,
    pb_escape,
    read_am,
    read_amop,
    read_amproc,
    read_opclass,
    read_opfamily,
    read_procs,
    read_namespaces,
    write_text,
    normalize_regproc,
    with_pbtxt_header,
)
from diagnostics import Diagnostics, DropReason


@dataclass(frozen=True)
class RegistriesConfig:
    """Registry pbtxt generation knobs (currently empty)."""
    pass


def _emit_access_methods(
    am: Dict[int, AmRow],
    *,
    stats,
) -> tuple[str, int]:
    out: List[str] = []
    out.append("engine_specific {")
    out.append('  payload_type: "floe.access_methods+proto"')
    out.append("  [floe.ext.floe_access_methods] {")
    count = 0
    for oid in sorted(am.keys()):
        row = am[oid]
        stats.record_read()
        if not is_system_oid(row.oid):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"access_method={row.oid}",
            )
            continue
        stats.record_system_pass()
        if not row.amname:
            stats.record_drop(
                DropReason.DROP_MISSING_DEP,
                f"access_method={row.oid} missing name",
            )
            continue
        out.append("    methods {")
        out.append(f"      oid: {row.oid}")
        out.append(f'      amname: "{pb_escape(row.amname)}"')
        out.append("    }")
        count += 1
        stats.record_emitted()
    out.append("  }")
    out.append("}")
    return "\n".join(out), count


def _emit_operator_families(
    opf: Dict[int, OpfamilyRow],
    *,
    stats,
) -> tuple[str, int]:
    out: List[str] = []
    out.append("engine_specific {")
    out.append('  payload_type: "floe.operator_families+proto"')
    out.append("  [floe.ext.floe_operator_families] {")
    count = 0
    for oid in sorted(opf.keys()):
        row = opf[oid]
        stats.record_read()
        if not is_system_oid(row.oid):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"opf={row.oid}",
            )
            continue
        stats.record_system_pass()
        if row.opfmethod is None:
            stats.record_drop(
                DropReason.DROP_MISSING_DEP,
                f"opf={row.oid} missing method",
            )
            continue
        out.append("    families {")
        out.append(f"      oid: {row.oid}")
        out.append(f"      opfmethod: {row.opfmethod}")
        if row.opfname is not None:
            out.append(f'      opfname: "{pb_escape(row.opfname)}"')
        if row.opfnamespace is not None:
            out.append(f"      opfnamespace: {row.opfnamespace}")
        out.append("    }")
        count += 1
        stats.record_emitted()
    out.append("  }")
    out.append("}")
    return "\n".join(out), count


def _emit_operator_classes(
    opc: Dict[int, OpclassRow],
    *,
    stats,
) -> tuple[str, int]:
    out: List[str] = []
    out.append("engine_specific {")
    out.append('  payload_type: "floe.operator_classes+proto"')
    out.append("  [floe.ext.floe_operator_classes] {")
    count = 0
    for oid in sorted(opc.keys()):
        row = opc[oid]
        stats.record_read()
        if not is_system_oid(row.oid):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"opc={row.oid}",
            )
            continue
        stats.record_system_pass()
        if row.opcfamily is None or row.opcintype is None or row.opcmethod is None:
            stats.record_drop(
                DropReason.DROP_MISSING_DEP,
                f"opc={row.oid} missing family/intype/method",
            )
            continue
        out.append("    classes {")
        out.append(f"      oid: {row.oid}")
        out.append(f"      opcfamily: {row.opcfamily}")
        out.append(f"      opcintype: {row.opcintype}")
        out.append(f"      opcmethod: {row.opcmethod}")
        if row.opcdefault is not None:
            out.append(f"      opcdefault: {'true' if row.opcdefault else 'false'}")
        if row.opcname is not None:
            out.append(f'      opcname: "{pb_escape(row.opcname)}"')
        if row.opcnamespace is not None:
            out.append(f"      opcnamespace: {row.opcnamespace}")
        out.append("    }")
        count += 1
        stats.record_emitted()
    out.append("  }")
    out.append("}")
    return "\n".join(out), count


def _emit_operator_access_methods(
    amop: List[AmopRow],
    *,
    stats,
) -> tuple[str, int]:
    out: List[str] = []
    out.append("engine_specific {")
    out.append('  payload_type: "floe.operator_access_methods+proto"')
    out.append("  [floe.ext.floe_operator_access_methods] {")
    count = 0

    def key(r: AmopRow) -> tuple[int, int, int, int, int]:
        return (
            r.amopfamily or 0,
            r.amopstrategy or 0,
            r.amoplefttype or 0,
            r.amoprighttype or 0,
            r.amopopr or 0,
        )

    for r in sorted(amop, key=key):
        stats.record_read()
        if r.amopfamily is None or r.amopstrategy is None or r.amopopr is None:
            stats.record_drop(
                DropReason.DROP_MISSING_DEP,
                f"amop family={r.amopfamily} strategy={r.amopstrategy} opr={r.amopopr}",
            )
            continue
        if not is_system_oid(r.amopopr):
            stats.record_drop(DropReason.DROP_NOT_SYSTEM_OID, f"amop opr={r.amopopr}")
            continue
        stats.record_system_pass()

        out.append("    entries {")
        out.append(f"      amopfamily: {r.amopfamily}")
        out.append(f"      amopstrategy: {r.amopstrategy}")
        if r.amoplefttype is not None:
            out.append(f"      amoplefttype: {r.amoplefttype}")
        if r.amoprighttype is not None:
            out.append(f"      amoprighttype: {r.amoprighttype}")
        out.append(f"      amopopr: {r.amopopr}")
        if r.amoppurpose is not None:
            out.append(f'      amoppurpose: "{pb_escape(r.amoppurpose)}"')
        if r.amopsortfamily is not None:
            out.append(f"      amopsortfamily: {r.amopsortfamily}")
        if r.amopmethod is not None:
            out.append(f"      amopmethod: {r.amopmethod}")
        out.append("    }")
        count += 1
        stats.record_emitted()

    out.append("  }")
    out.append("}")
    return "\n".join(out), count


def _emit_procedures_access_methods(
    amproc: List[AmprocRow],
    *,
    proc_name_to_oid: Dict[str, int],
    stats,
) -> tuple[str, int]:
    out: List[str] = []
    out.append("engine_specific {")
    out.append('  payload_type: "floe.procedure_access_methods+proto"')
    out.append("  [floe.ext.floe_procedure_access_methods] {")
    count = 0

    def key(r: AmprocRow) -> tuple[int, int, int, int, int, str]:
        return (
            r.amprocfamily or 0,
            r.amprocnum or 0,
            r.amproclefttype or 0,
            r.amprocrighttype or 0,
            r.amproc_oid or 0,
            r.amproc_name or "",
        )

    for r in sorted(amproc, key=key):
        stats.record_read()
        if r.amprocfamily is None or r.amprocnum is None:
            stats.record_drop(
                DropReason.DROP_MISSING_DEP,
                f"amproc missing family/num {r}",
            )
            continue

        func_oid: Optional[int] = r.amproc_oid
        if func_oid is None and r.amproc_name:
            for key in normalize_regproc(r.amproc_name):
                func_oid = proc_name_to_oid.get(key)
                if func_oid:
                    break

        # If we still can't resolve, skip (we must emit a numeric OID)
        if func_oid is None:
            stats.record_drop(
                DropReason.DROP_UNRESOLVED_REGPROC,
                f"amproc name={r.amproc_name}",
            )
            continue

        # system-only filter: apply on resolved function oid
        if not is_system_oid(func_oid):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"amproc func_oid={func_oid}",
            )
            continue
        stats.record_system_pass()

        out.append("    entries {")
        out.append(f"      amprocfamily: {r.amprocfamily}")
        out.append(f"      amprocnum: {r.amprocnum}")
        if r.amproclefttype is not None:
            out.append(f"      amproclefttype: {r.amproclefttype}")
        if r.amprocrighttype is not None:
            out.append(f"      amprocrighttype: {r.amprocrighttype}")
        out.append(f"      amproc: {func_oid}")
        out.append("    }")
        count += 1
        stats.record_emitted()

    out.append("  }")
    out.append("}")
    return "\n".join(out), count


def generate_registries_pbtxt(
    *,
    csv_dir: Path,
    config: RegistriesConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, Dict[str, int]]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("registries")
    # Needed for resolving procs-by-name (regproc pretty-print)
    namespaces = read_namespaces(csv_dir)
    procs_by_oid = read_procs(csv_dir)
    proc_name_to_oid = build_proc_name_to_oid_map(procs_by_oid, namespaces)

    am = read_am(csv_dir)
    opf = read_opfamily(csv_dir)
    opc = read_opclass(csv_dir)
    amop = read_amop(csv_dir)
    amproc = read_amproc(csv_dir)

    am_block, am_count = _emit_access_methods(
        am, stats=stats
    )
    opf_block, opf_count = _emit_operator_families(
        opf, stats=stats
    )
    opc_block, opc_count = _emit_operator_classes(
        opc, stats=stats
    )
    amop_block, amop_count = _emit_operator_access_methods(
        amop, stats=stats
    )
    proc_block, proc_count = _emit_procedures_access_methods(
        amproc,
        proc_name_to_oid=proc_name_to_oid,
        stats=stats,
    )

    blocks = [am_block, opf_block, opc_block, amop_block, proc_block]
    summary = [
        "# registry counts:",
        f"#   access_methods: {am_count}",
        f"#   operator_families: {opf_count}",
        f"#   operator_classes: {opc_count}",
        f"#   operator_access_methods: {amop_count}",
        f"#   procedure_access_methods: {proc_count}",
    ]

    content = "\n\n".join(blocks) + "\n\n" + "\n".join(summary) + "\n"
    counts = {
        "access_methods": am_count,
        "operator_families": opf_count,
        "operator_classes": opc_count,
        "operator_access_methods": amop_count,
        "procedure_access_methods": proc_count,
    }
    return with_pbtxt_header(content), counts


def write_registries_pbtxt(
    *,
    csv_dir: Path,
    out_path: Path,
    config: RegistriesConfig,
    diagnostics: Diagnostics | None = None,
) -> Dict[str, int]:
    content, counts = generate_registries_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return counts


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 registries_pbtxt.py <csv_dir> <out_registries_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_registries_pbtxt(csv_dir=csv_dir, out_path=out_path, config=RegistriesConfig())
    print(f"Wrote {out_path}")
