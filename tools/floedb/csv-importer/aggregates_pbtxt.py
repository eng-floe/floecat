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
Generate FloeCat PBtxt for aggregates from pg_aggregate.csv (system only).

Inputs:
- pg_aggregate.csv
- pg_proc.csv
- pg_type.csv
- pg_namespace.csv

Output blocks like:

aggregates {
  name { name: "sum" path: "pg_catalog" }
  argument_types { name: "int4" path: "pg_catalog" }
  return_type { name: "int8" path: "pg_catalog" }
  state_type { name: "int8" path: "pg_catalog" }
  engine_specific {
    payload_type: "floe.aggregate+proto"
    [floe.ext.floe_aggregate] {
      aggfnoid: 2108
      aggkind: "n"
      aggtransfn: 1844
      aggfinalfn: 0
      aggtranstype: 20
    }
  }
}

Notes:
- system-only filter: use aggfnoid < 16384 (and we also require the pg_proc row)
- namespace filtering: optional keep only pg_catalog via include_non_pg_catalog flag
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from utils import (
    AggRow,
    NamespaceRow,
    ProcRow,
    TypeRow,
    build_proc_name_to_oid_map,
    is_system_oid,
    pb_escape,
    read_aggregates,
    read_namespaces,
    read_procs,
    read_types,
    resolve_namespace_name,
    write_text,
    normalize_regproc,
    with_pbtxt_header,
)
from diagnostics import Diagnostics, DropReason


@dataclass(frozen=True)
class AggregatesConfig:
    include_non_pg_catalog: bool = True


def _name_ref(obj_name: str, namespace_name: str) -> str:
    return f'{{ name: "{pb_escape(obj_name)}" path: "{pb_escape(namespace_name)}" }}'


def _type_name_ref(
    types_by_oid: Dict[int, TypeRow],
    namespaces: Dict[int, NamespaceRow],
    type_oid: int,
) -> str:
    t = types_by_oid.get(type_oid)
    if not t or not t.typname:
        # conservative fallback: still emit something stable-ish
        return _name_ref(f"oid_{type_oid}", "pg_catalog")
    ns = resolve_namespace_name(namespaces, t.typnamespace)
    return _name_ref(t.typname, ns)


def _emit_aggregate_block(
    a: AggRow,
    p: ProcRow,
    namespaces: Dict[int, NamespaceRow],
    types_by_oid: Dict[int, TypeRow],
    func_oid: int,
) -> str:
    ns_name = resolve_namespace_name(namespaces, p.pronamespace or 0)

    # Signature from pg_proc (already parsed as List[int])
    arg_oids = p.proargtypes
    arg_refs = [_type_name_ref(types_by_oid, namespaces, oid) for oid in arg_oids]

    return_ref = _type_name_ref(types_by_oid, namespaces, p.prorettype)
    state_ref = _type_name_ref(types_by_oid, namespaces, a.aggtranstype or 0)

    out: List[str] = []
    out.append("aggregates {")
    out.append(f"  name {_name_ref(p.proname, ns_name)}")
    for r in arg_refs:
        out.append(f"  argument_types {r}")
    out.append(f"  return_type {return_ref}")
    out.append(f"  state_type {state_ref}")

    # Engine-specific extension
    out.append("  engine_specific {")
    out.append('    payload_type: "floe.aggregate+proto"')
    out.append("    [floe.ext.floe_aggregate] {")
    out.append(f"      aggfnoid: {func_oid}")

    if a.aggkind is not None:
        out.append(f'      aggkind: "{pb_escape(a.aggkind)}"')
    if a.aggnumdirectargs is not None:
        out.append(f"      aggnumdirectargs: {a.aggnumdirectargs}")
    if a.aggtransfn is not None:
        out.append(f"      aggtransfn: {a.aggtransfn}")
    if a.aggfinalfn is not None:
        out.append(f"      aggfinalfn: {a.aggfinalfn}")
    if a.aggmtransfn is not None:
        out.append(f"      aggmtransfn: {a.aggmtransfn}")
    if a.aggminvtransfn is not None:
        out.append(f"      aggminvtransfn: {a.aggminvtransfn}")
    if a.aggmfinalfn is not None:
        out.append(f"      aggmfinalfn: {a.aggmfinalfn}")
    if a.aggfinalextra is not None:
        out.append(f"      aggfinalextra: {'true' if a.aggfinalextra else 'false'}")
    if a.aggmfinalextra is not None:
        out.append(f"      aggmfinalextra: {'true' if a.aggmfinalextra else 'false'}")
    if a.aggsortop is not None:
        out.append(f"      aggsortop: {a.aggsortop}")
    if a.aggtranstype is not None:
        out.append(f"      aggtranstype: {a.aggtranstype}")
    if a.aggtransspace is not None:
        out.append(f"      aggtransspace: {a.aggtransspace}")
    if a.aggmtranstype is not None:
        out.append(f"      aggmtranstype: {a.aggmtranstype}")
    if a.aggmtransspace is not None:
        out.append(f"      aggmtransspace: {a.aggmtransspace}")
    if a.agginitval is not None:
        out.append(f'      agginitval: "{pb_escape(a.agginitval)}"')
    if a.aggminitval is not None:
        out.append(f'      aggminitval: "{pb_escape(a.aggminitval)}"')

    out.append("    }")
    out.append("  }")
    out.append("}")
    return "\n".join(out)


def generate_aggregates_pbtxt(
    csv_dir: Path,
    config: AggregatesConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, int]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("aggregates")
    namespaces = read_namespaces(csv_dir)
    types_by_oid = read_types(csv_dir)
    procs_by_oid = read_procs(csv_dir)
    proc_name_to_oid = build_proc_name_to_oid_map(procs_by_oid, namespaces)
    aggs = read_aggregates(csv_dir)

    blocks: List[str] = []

    for a in sorted(aggs, key=lambda row: (row.aggfnoid or 0, row.aggfnoid_name or "")):
        stats.record_read()
        func_oid = a.aggfnoid
        if func_oid is None and a.aggfnoid_name:
            for key in normalize_regproc(a.aggfnoid_name):
                func_oid = proc_name_to_oid.get(key)
                if func_oid:
                    break
        if func_oid is None:
            stats.record_drop(
                DropReason.DROP_UNRESOLVED_REGPROC,
                f"aggregate={a.aggfnoid_name or 'unknown'}",
            )
            continue
        if not is_system_oid(func_oid):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"aggregate func_oid={func_oid}",
            )
            continue
        stats.record_system_pass()

        p = procs_by_oid.get(func_oid)
        if not p:
            stats.record_drop(
                DropReason.DROP_MISSING_PG_PROC,
                f"agg func_oid={func_oid}",
            )
            continue

        ns_name = resolve_namespace_name(namespaces, p.pronamespace or 0)
        if not config.include_non_pg_catalog and ns_name != "pg_catalog":
            stats.record_drop(
                DropReason.DROP_NAMESPACE_FILTER,
                f"aggregates proc ns={ns_name}",
            )
            continue

        blocks.append(
            _emit_aggregate_block(
                a=a,
                p=p,
                namespaces=namespaces,
                types_by_oid=types_by_oid,
                func_oid=func_oid,
            )
        )
        stats.record_emitted()

    content = "\n\n".join(blocks) + ("\n" if blocks else "")
    content += f"# aggregates count: {len(blocks)}\n"
    return with_pbtxt_header(content), len(blocks)


def write_aggregates_pbtxt(
    csv_dir: Path,
    out_path: Path,
    config: AggregatesConfig,
    diagnostics: Diagnostics | None = None,
) -> int:
    content, count = generate_aggregates_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return count


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 aggregates_pbtxt.py <csv_dir> <out_aggregates_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_aggregates_pbtxt(csv_dir=csv_dir, out_path=out_path, config=AggregatesConfig())
    print(f"Wrote {out_path}")
