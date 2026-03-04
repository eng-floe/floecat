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
Generate Floecat PBtxt for SQL casts from pg_cast.csv (system only).

Emits blocks like:

casts {
  name { name: "cast_int4_to_text" path: "pg_catalog" }
  source_type { name: "int4" path: "pg_catalog" }
  target_type { name: "text" path: "pg_catalog" }
  method: "implicit"
  engine_specific {
    payload_type: "floe.cast+proto"
    [floe.ext.floe_cast] {
      castsource: 23
      casttarget: 25
      castfunc: 0
      castcontext: "i"
      castmethod: "f"
    }
  }
}

Notes:
- system-only filter: castsource < 16384 AND casttarget < 16384
- cast "name" is synthetic/stable: "cast_<src>_to_<dst>" in the target namespace (usually pg_catalog)
- SqlCast.method is mapped from pg_cast.castcontext:
    'i' -> "implicit"
    'a' -> "assignment"
    'e' -> "explicit"
- engine_specific captures raw pg_cast fields
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from utils import (
    CastRow,
    NamespaceRow,
    TypeRow,
    is_system_oid,
    pb_escape,
    read_casts,
    read_namespaces,
    read_types,
    resolve_namespace_name,
    write_text,
    with_pbtxt_header,
)
from diagnostics import Diagnostics, DropReason


@dataclass(frozen=True)
class CastsConfig:
    include_non_pg_catalog: bool = True


def _name_ref(obj_name: str, namespace_name: str) -> str:
    return f'{{ name: "{pb_escape(obj_name)}" path: "{pb_escape(namespace_name)}" }}'


def _cast_context_to_method(ctx: Optional[str]) -> str:
    if ctx == "i":
        return "implicit"
    if ctx == "a":
        return "assignment"
    if ctx == "e":
        return "explicit"
    return "explicit"


def _emit_cast_block(
    *,
    c: CastRow,
    types: Dict[int, TypeRow],
    namespaces: Dict[int, NamespaceRow],
) -> Optional[str]:
    src = types.get(c.castsource or 0)
    dst = types.get(c.casttarget or 0)

    if not src or not dst or not src.typname or not dst.typname:
        return None

    src_ns = resolve_namespace_name(namespaces, src.typnamespace)
    dst_ns = resolve_namespace_name(namespaces, dst.typnamespace)

    # Synthetic cast name (stable & readable)
    # Put the name in the *target* namespace (generally pg_catalog).
    cast_name = f"cast_{src.typname}_to_{dst.typname}"
    cast_name_ns = dst_ns

    out: List[str] = []
    out.append("casts {")
    out.append(f"  name {_name_ref(cast_name, cast_name_ns)}")
    out.append(f"  source_type {_name_ref(src.typname, src_ns)}")
    out.append(f"  target_type {_name_ref(dst.typname, dst_ns)}")
    out.append(f'  method: "{pb_escape(_cast_context_to_method(c.castcontext))}"')

    out.append("  engine_specific {")
    out.append('    payload_type: "floe.cast+proto"')
    out.append("    [floe.ext.floe_cast] {")
    out.append(f"      castsource: {c.castsource}")
    out.append(f"      casttarget: {c.casttarget}")

    if (c.castfunc or 0) != 0:
        out.append(f"      castfunc: {c.castfunc}")
    if c.castcontext is not None:
        out.append(f'      castcontext: "{pb_escape(c.castcontext)}"')
    if c.castmethod is not None:
        out.append(f'      castmethod: "{pb_escape(c.castmethod)}"')

    out.append("    }")
    out.append("  }")
    out.append("}")
    return "\n".join(out)


def generate_casts_pbtxt(
    *,
    csv_dir: Path,
    config: CastsConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, int]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("casts")

    namespaces = read_namespaces(csv_dir)
    types = read_types(csv_dir)
    casts = read_casts(csv_dir)

    blocks: List[str] = []

    # Sort by (source,target) for stability
    for key in sorted(casts.keys()):
        c = casts[key]
        stats.record_read()

        if not (is_system_oid(c.castsource) and is_system_oid(c.casttarget)):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"cast={key} source={c.castsource} target={c.casttarget}",
            )
            continue
        stats.record_system_pass()

        src = types.get(c.castsource or 0)
        dst = types.get(c.casttarget or 0)
        if not src or not dst or not src.typname or not dst.typname:
            stats.record_drop(
                DropReason.DROP_MISSING_TYPE,
                f"cast={key} missing type metadata",
            )
            continue

        # Namespace filter: apply on both sides (conservative)
        if not config.include_non_pg_catalog:
            src_ns = resolve_namespace_name(namespaces, src.typnamespace)
            dst_ns = resolve_namespace_name(namespaces, dst.typnamespace)
            if src_ns != "pg_catalog":
                stats.record_drop(
                    DropReason.DROP_NAMESPACE_FILTER,
                    f"cast={key} source_ns={src_ns}",
                )
                continue
            if dst_ns != "pg_catalog":
                stats.record_drop(
                    DropReason.DROP_NAMESPACE_FILTER,
                    f"cast={key} target_ns={dst_ns}",
                )
                continue

        b = _emit_cast_block(c=c, types=types, namespaces=namespaces)
        if b:
            blocks.append(b)
            stats.record_emitted()
        else:
            stats.record_drop(
                DropReason.DROP_MISSING_TYPE,
                f"cast={key} emit block failed",
            )

    content = "\n\n".join(blocks) + ("\n" if blocks else "")
    content += f"# casts count: {len(blocks)}\n"
    return with_pbtxt_header(content), len(blocks)


def write_casts_pbtxt(
    *,
    csv_dir: Path,
    out_path: Path,
    config: CastsConfig,
    diagnostics: Diagnostics | None = None,
) -> int:
    content, count = generate_casts_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return count


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 casts_pbtxt.py <csv_dir> <out_casts_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_casts_pbtxt(csv_dir=csv_dir, out_path=out_path, config=CastsConfig())
    print(f"Wrote {out_path}")
