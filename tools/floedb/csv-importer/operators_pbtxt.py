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
Generate FloeCat PBtxt for SQL operators from pg_operator.csv (system only).

Emits blocks like:

operators {
  name { name: "=" path: "pg_catalog" }
  left_type  { name: "int4" path: "pg_catalog" }
  right_type { name: "int4" path: "pg_catalog" }
  return_type { name: "bool" path: "pg_catalog" }
  engine_specific {
    payload_type: "floe.operator+proto"
    [floe.ext.floe_operator] {
      oid: 65
      oprname: "="
      oprnamespace: 11
      oprkind: "b"
      oprcanmerge: true
      oprcanhash: true
      oprleft: 23
      oprright: 23
      oprresult: 16
      oprcom: 65
      oprnegate: 144
      oprcode: 65
      oprrest: 0
      oprjoin: 0
    }
  }
}

Policy:
- system-only filter: oid < 16384
- default: include all system namespaces; --pg-catalog-only restricts to pg_catalog only
- resolves operand/result types via pg_type OID -> typname, and uses the type's namespace
- leaves is_commutative/is_associative unset (default false in proto3) unless you want heuristics later
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from utils import (
    NamespaceRow,
    OperatorRow,
    TypeRow,
    is_system_oid,
    pb_escape,
    read_namespaces,
    read_operators,
    read_types,
    resolve_namespace_name,
    write_text,
    with_pbtxt_header,
)
from diagnostics import Diagnostics, DropReason


@dataclass(frozen=True)
class OperatorsConfig:
    include_non_pg_catalog: bool = True


def _name_ref(obj_name: str, namespace_name: str) -> str:
    return f'{{ name: "{pb_escape(obj_name)}" path: "{pb_escape(namespace_name)}" }}'


def _emit_operator_block(
    *,
    o: OperatorRow,
    op_namespace_name: str,
    types: Dict[int, TypeRow],
    namespaces: Dict[int, NamespaceRow],
) -> Optional[str]:
    # Resolve types (left/right can be 0 for unary operators; in PG, unary uses oprleft=0 or oprright=0)
    left_t = types.get(o.oprleft) if (o.oprleft or 0) != 0 else None
    right_t = types.get(o.oprright) if (o.oprright or 0) != 0 else None
    result_t = types.get(o.oprresult) if (o.oprresult or 0) != 0 else None

    if not result_t or not result_t.typname:
        return None

    out: List[str] = []
    out.append("operators {")
    out.append(f"  name {_name_ref(o.oprname, op_namespace_name)}")

    if left_t and left_t.typname:
        ns = resolve_namespace_name(namespaces, left_t.typnamespace)
        out.append(f"  left_type {_name_ref(left_t.typname, ns)}")
    if right_t and right_t.typname:
        ns = resolve_namespace_name(namespaces, right_t.typnamespace)
        out.append(f"  right_type {_name_ref(right_t.typname, ns)}")

    res_ns = resolve_namespace_name(namespaces, result_t.typnamespace)
    out.append(f"  return_type {_name_ref(result_t.typname, res_ns)}")

    # Engine-specific extension
    out.append("  engine_specific {")
    out.append('    payload_type: "floe.operator+proto"')
    out.append("    [floe.ext.floe_operator] {")

    out.append(f"      oid: {o.oid}")
    out.append(f'      oprname: "{pb_escape(o.oprname)}"')
    if o.oprnamespace is not None:
        out.append(f"      oprnamespace: {o.oprnamespace}")
    if o.oprkind is not None:
        out.append(f'      oprkind: "{pb_escape(o.oprkind)}"')
    if o.oprcanmerge is not None:
        out.append(f"      oprcanmerge: {'true' if o.oprcanmerge else 'false'}")
    if o.oprcanhash is not None:
        out.append(f"      oprcanhash: {'true' if o.oprcanhash else 'false'}")

    if (o.oprleft or 0) != 0:
        out.append(f"      oprleft: {o.oprleft}")
    if (o.oprright or 0) != 0:
        out.append(f"      oprright: {o.oprright}")
    if (o.oprresult or 0) != 0:
        out.append(f"      oprresult: {o.oprresult}")

    if (o.oprcom or 0) != 0:
        out.append(f"      oprcom: {o.oprcom}")
    if (o.oprnegate or 0) != 0:
        out.append(f"      oprnegate: {o.oprnegate}")

    if (o.oprcode or 0) != 0:
        out.append(f"      oprcode: {o.oprcode}")
    if (o.oprrest or 0) != 0:
        out.append(f"      oprrest: {o.oprrest}")
    if (o.oprjoin or 0) != 0:
        out.append(f"      oprjoin: {o.oprjoin}")

    out.append("    }")
    out.append("  }")
    out.append("}")

    return "\n".join(out)


def generate_operators_pbtxt(
    *,
    csv_dir: Path,
    config: OperatorsConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, int]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("operators")

    namespaces = read_namespaces(csv_dir)
    types = read_types(csv_dir)
    ops = read_operators(csv_dir)

    blocks: List[str] = []

    for oid in sorted(ops.keys()):
        o = ops[oid]
        stats.record_read()
        if not is_system_oid(o.oid):
            stats.record_drop(DropReason.DROP_NOT_SYSTEM_OID, f"opr={o.oid}")
            continue
        stats.record_system_pass()

        op_ns = resolve_namespace_name(namespaces, o.oprnamespace or 0)
        if not config.include_non_pg_catalog and op_ns != "pg_catalog":
            stats.record_drop(
                DropReason.DROP_NAMESPACE_FILTER,
                f"opr={o.oid} namespace={op_ns}",
            )
            continue

        b = _emit_operator_block(
            o=o,
            op_namespace_name=op_ns,
            types=types,
            namespaces=namespaces,
        )
        if b:
            blocks.append(b)
            stats.record_emitted()
        else:
            stats.record_drop(
                DropReason.DROP_MISSING_TYPE,
                f"opr={o.oid} missing return type",
            )

    content = "\n\n".join(blocks) + ("\n" if blocks else "")
    content += f"# operators count: {len(blocks)}\n"
    return with_pbtxt_header(content), len(blocks)


def write_operators_pbtxt(
    *,
    csv_dir: Path,
    out_path: Path,
    config: OperatorsConfig,
    diagnostics: Diagnostics | None = None,
) -> int:
    content, count = generate_operators_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return count


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 operators_pbtxt.py <csv_dir> <out_operators_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_operators_pbtxt(
        csv_dir=csv_dir,
        out_path=out_path,
        config=OperatorsConfig(),
    )
    print(f"Wrote {out_path}")
