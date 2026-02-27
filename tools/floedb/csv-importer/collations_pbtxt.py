#!/usr/bin/env python3
# /*
#  * Copyright 2026 Yellowbrick Data, Inc.
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
"""
Generate Floecat PBtxt for collations from pg_collation.csv (system only).

Emits blocks like:

collations {
  name { name: "default" path: "pg_catalog" }
  locale: "en_US.utf8"
  engine_specific {
    payload_type: "floe.collation+proto"
    [floe.ext.floe_collation] {
      oid: 100
      collname: "default"
      collnamespace: 11
      collencoding: -1
      collcollate: "en_US.utf8"
      collctype: "en_US.utf8"
    }
  }
}

Notes:
- system-only filter: oid < 16384
- locale is derived from collcollate/collctype:
    - if both present and equal -> that string
    - else "collate=<..>;ctype=<..>" (only include those that exist)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from diagnostics import Diagnostics, DropReason
from utils import (
    CollationRow,
    NamespaceRow,
    is_system_oid,
    pb_escape,
    read_collations,
    read_namespaces,
    resolve_namespace_name,
    write_text,
    with_pbtxt_header,
)


@dataclass(frozen=True)
class CollationsConfig:
    include_non_pg_catalog: bool = True


def _name_ref(obj_name: str, namespace_name: str) -> str:
    return f'{{ name: "{pb_escape(obj_name)}" path: "{pb_escape(namespace_name)}" }}'


def _derive_locale(c: CollationRow) -> str:
    collate = (c.collcollate or "").strip()
    ctype = (c.collctype or "").strip()

    if collate and ctype and collate == ctype:
        return collate

    parts: List[str] = []
    if collate:
        parts.append(f"collate={collate}")
    if ctype:
        parts.append(f"ctype={ctype}")

    return ";".join(parts) if parts else ""


def _emit_collation_block(*, c: CollationRow, namespace_name: str) -> str:
    out: List[str] = []
    out.append("collations {")
    out.append(f"  name {_name_ref(c.collname, namespace_name)}")

    locale = _derive_locale(c)
    if locale:
        out.append(f'  locale: "{pb_escape(locale)}"')

    out.append("  engine_specific {")
    out.append('    payload_type: "floe.collation+proto"')
    out.append("    [floe.ext.floe_collation] {")
    out.append(f"      oid: {c.oid}")
    out.append(f'      collname: "{pb_escape(c.collname)}"')
    out.append(f"      collnamespace: {c.collnamespace}")

    if c.collencoding is not None:
        out.append(f"      collencoding: {c.collencoding}")
    if c.collcollate is not None:
        out.append(f'      collcollate: "{pb_escape(c.collcollate)}"')
    if c.collctype is not None:
        out.append(f'      collctype: "{pb_escape(c.collctype)}"')

    out.append("    }")
    out.append("  }")
    out.append("}")
    return "\n".join(out)


def generate_collations_pbtxt(
    *,
    csv_dir: Path,
    config: CollationsConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, int]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("collations")

    namespaces = read_namespaces(csv_dir)
    collations = read_collations(csv_dir)

    blocks: List[str] = []
    for oid in sorted(collations.keys()):
        c = collations[oid]
        stats.record_read()

        if not is_system_oid(c.oid):
            stats.record_drop(
                DropReason.DROP_NOT_SYSTEM_OID,
                f"collation={c.collname} oid={c.oid}",
            )
            continue
        stats.record_system_pass()

        namespace_name = resolve_namespace_name(namespaces, c.collnamespace)
        if not config.include_non_pg_catalog and namespace_name != "pg_catalog":
            stats.record_drop(
                DropReason.DROP_NAMESPACE_FILTER,
                f"collation={c.collname} namespace={namespace_name}",
            )
            continue

        blocks.append(_emit_collation_block(c=c, namespace_name=namespace_name))
        stats.record_emitted()

    content = "\n\n".join(blocks) + ("\n" if blocks else "")
    content += f"# collations count: {len(blocks)}\n"
    return with_pbtxt_header(content), len(blocks)


def write_collations_pbtxt(
    *,
    csv_dir: Path,
    out_path: Path,
    config: CollationsConfig,
    diagnostics: Diagnostics | None = None,
) -> int:
    content, count = generate_collations_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return count


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 collations_pbtxt.py <csv_dir> <out_collations_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_collations_pbtxt(csv_dir=csv_dir, out_path=out_path, config=CollationsConfig())
    print(f"Wrote {out_path}")
