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
Generate FloeCat PBtxt for SQL functions from pg_proc.csv (system only).

Emits blocks like:

functions {
  name { name: "int4_abs" path: "pg_catalog" }
  argument_types { name: "int4" path: "pg_catalog" }
  return_type { name: "int4" path: "pg_catalog" }
  engine_specific {
    payload_type: "floe.function+proto"
    [floe.ext.floe_function] {
      oid: 1250
      proname: "int4_abs"
      prolang: 12
      proisstrict: true
      proleakproof: false
      proisagg: false
      proiswindow: false
      provolatile: "i"
      prosrc: "int4_abs"
      pronargs: 1
      prorettype: 23
      proargtypes: 23
      proargtypelen: 1
    }
  }
}

Policy:
- system-only filter: oid < 16384
- by default includes objects from all system namespaces unless --pg-catalog-only
- resolves return/argument types via pg_type (OID -> typname)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from utils import (
    NamespaceRow,
    ProcRow,
    TypeRow,
    is_system_oid,
    pb_escape,
    read_namespaces,
    read_procs,
    read_types,
    resolve_namespace_name,
    write_text,
    with_pbtxt_header,
)
from diagnostics import Diagnostics, DropReason


@dataclass(frozen=True)
class FunctionsConfig:
    include_non_pg_catalog: bool = True
    include_prosrc: bool = False
    include_variadic: bool = False


def _name_ref(name: str, namespace_name: str) -> str:
    return f'{{ name: "{pb_escape(name)}" path: "{pb_escape(namespace_name)}" }}'


def _emit_function_block(
    *,
    p: ProcRow,
    proc_namespace_name: str,
    type_by_oid: Dict[int, TypeRow],
    namespaces: Dict[int, NamespaceRow],
    config: FunctionsConfig,
) -> Optional[str]:
    ret_type = type_by_oid.get(p.prorettype)
    if not ret_type or not ret_type.typname:
        return None

    ret_ns = resolve_namespace_name(namespaces, ret_type.typnamespace)

    # Resolve arg types (already parsed as List[int] by read_procs())
    arg_types: List[TypeRow] = []
    for oid in p.proargtypes:
        t = type_by_oid.get(oid)
        if t and t.typname:
            arg_types.append(t)

    # Optional variadic element type
    if config.include_variadic and (p.provariadic or 0) != 0:
        t = type_by_oid.get(p.provariadic or 0)
        if t and t.typname:
            arg_types.append(t)

    out: List[str] = []
    out.append("functions {")
    out.append(f"  name {_name_ref(p.proname, proc_namespace_name)}")

    for t in arg_types:
        ns = resolve_namespace_name(namespaces, t.typnamespace)
        out.append(f"  argument_types {_name_ref(t.typname, ns)}")

    out.append(f"  return_type {_name_ref(ret_type.typname, ret_ns)}")

    if p.proisagg:
        out.append("  is_aggregate: true")
    if p.proiswindow:
        out.append("  is_window: true")

    out.append("  engine_specific {")
    out.append('    payload_type: "floe.function+proto"')
    out.append("    [floe.ext.floe_function] {")
    out.append(f"      oid: {p.oid}")
    out.append(f'      proname: "{pb_escape(p.proname)}"')

    if p.pronamespace is not None:
        out.append(f"      pronamespace: {p.pronamespace}")
    if p.proowner is not None:
        out.append(f"      proowner: {p.proowner}")
    if p.prolang is not None:
        out.append(f"      prolang: {p.prolang}")
    if p.procost is not None:
        out.append(f"      procost: {p.procost}")
    if p.prorows is not None:
        out.append(f"      prorows: {p.prorows}")
    if p.provariadic is not None and p.provariadic != 0:
        out.append(f"      provariadic: {p.provariadic}")

    out.append(f"      proisagg: {'true' if p.proisagg else 'false'}")
    out.append(f"      proiswindow: {'true' if p.proiswindow else 'false'}")

    if p.prosecdef is not None:
        out.append(f"      prosecdef: {'true' if p.prosecdef else 'false'}")
    if p.proleakproof is not None:
        out.append(f"      proleakproof: {'true' if p.proleakproof else 'false'}")
    if p.proisstrict is not None:
        out.append(f"      proisstrict: {'true' if p.proisstrict else 'false'}")
    if p.proretset is not None:
        out.append(f"      proretset: {'true' if p.proretset else 'false'}")

    if p.provolatile is not None:
        out.append(f'      provolatile: "{pb_escape(p.provolatile)}"')

    if config.include_prosrc and p.prosrc is not None:
        out.append(f'      prosrc: "{pb_escape(p.prosrc)}"')

    if p.pronargs is not None:
        out.append(f"      pronargs: {p.pronargs}")

    out.append(f"      prorettype: {p.prorettype}")

    for t in arg_types:
        out.append(f"      proargtypes: {t.oid}")
    out.append(f"      proargtypelen: {len(arg_types)}")

    out.append("    }")
    out.append("  }")
    out.append("}")
    return "\n".join(out)


def generate_functions_pbtxt(
    *,
    csv_dir: Path,
    config: FunctionsConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, int]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("functions")

    namespaces = read_namespaces(csv_dir)
    types = read_types(csv_dir)
    procs = read_procs(csv_dir)
    seen_signatures: Set[Tuple[str, str, int, Tuple[int, ...]]] = set()

    blocks: List[str] = []
    for oid in sorted(procs.keys()):
        p = procs[oid]
        stats.record_read()
        if not is_system_oid(p.oid):
            stats.record_drop(DropReason.DROP_NOT_SYSTEM_OID, f"proc={p.oid}")
            continue
        stats.record_system_pass()

        proc_ns = resolve_namespace_name(namespaces, p.pronamespace or 0)
        if not config.include_non_pg_catalog and proc_ns != "pg_catalog":
            stats.record_drop(
                DropReason.DROP_NAMESPACE_FILTER,
                f"proc={p.oid} namespace={proc_ns}",
            )
            continue

        normalized_ns = proc_ns or ""
        signature = (normalized_ns, p.proname, p.prorettype, tuple(p.proargtypes))
        if signature in seen_signatures:
            stats.record_drop(
                DropReason.DROP_DUPLICATE_SIGNATURE,
                f"proc={p.oid} signature={signature}",
            )
            continue
        seen_signatures.add(signature)

        b = _emit_function_block(
            p=p,
            proc_namespace_name=proc_ns,
            type_by_oid=types,
            namespaces=namespaces,
            config=config,
        )
        if b:
            blocks.append(b)
            stats.record_emitted()
        else:
            stats.record_drop(
                DropReason.DROP_MISSING_TYPE,
                f"proc={p.oid} missing return type",
            )

    content = "\n\n".join(blocks) + ("\n" if blocks else "")
    content += f"# functions count: {len(blocks)}\n"
    return with_pbtxt_header(content), len(blocks)


def write_functions_pbtxt(
    *,
    csv_dir: Path,
    out_path: Path,
    config: FunctionsConfig,
    diagnostics: Diagnostics | None = None,
) -> int:
    content, count = generate_functions_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return count


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 functions_pbtxt.py <csv_dir> <out_functions_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_functions_pbtxt(csv_dir=csv_dir, out_path=out_path, config=FunctionsConfig())
    print(f"Wrote {out_path}")
