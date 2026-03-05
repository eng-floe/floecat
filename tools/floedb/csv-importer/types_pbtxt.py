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
Generate Floecat PBtxt for SQL types from pg_type.csv (system only).

Also emits FloeTypePlanningSemantics per type using:
- pg_am.csv
- pg_opclass.csv
- pg_opfamily.csv
- pg_amop.csv
- pg_amproc.csv

Semantics fields (best-effort):
- default_collation_oid from pg_type.typcollation
- preferred btree/hash opclass + opfamily via pg_opclass (opcdefault)
- eq/lt/gt operator oids via pg_amop (btree strategies 3/1/5)
- btree_cmp_proc_oid via pg_amproc (proc #1)
- hash_proc_oid via pg_amproc (proc #1)

pg_type completeness:
This generator also attempts to emit additional pg_type fields that are often
required downstream, including typinput/typoutput/typreceive/typsend and
typmodin/typmodout. Exporters sometimes emit these as numeric OIDs or as regproc
names; we resolve names against pg_proc (system-only) in a deterministic way.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from utils import (
    AmRow,
    AmopRow,
    AmprocRow,
    NamespaceRow,
    OpclassRow,
    ProcRow,
    TypeRow,
    is_array_type,
    is_system_oid,
    is_table_row_type,
    normalize_regproc,
    pb_escape,
    read_am,
    read_amop,
    read_amproc,
    read_namespaces,
    read_opclass,
    read_types,
    read_procs,
    resolve_namespace_name,
    build_proc_name_to_oid_map,
    type_category_code,
    write_text,
    with_pbtxt_header,
)
from diagnostics import Diagnostics, DropReason


@dataclass(frozen=True)
class TypesConfig:
    include_non_pg_catalog: bool = True
    include_table_rowtypes: bool = False
    include_arrays: bool = True


def _collect_required_type_oids(procs: Dict[int, ProcRow]) -> Set[int]:
    required: Set[int] = set()
    for p in procs.values():
        if p.prorettype:
            required.add(p.prorettype)
        required.update(x for x in p.proargtypes if x)
        if p.provariadic:
            required.add(p.provariadic)
    return required


def _array_name_matches_element(array_name: str, element_name: str) -> bool:
    if not array_name or not element_name:
        return False
    return array_name.lower() == f"_{element_name.lower()}"


def _should_replace_array(
    existing_oid: int,
    new_oid: int,
    element_name: str,
    types: Dict[int, TypeRow],
) -> bool:
    existing_row = types.get(existing_oid)
    new_row = types.get(new_oid)
    existing_name = existing_row.typname if existing_row else ""
    new_name = new_row.typname if new_row else ""
    new_pref = _array_name_matches_element(new_name, element_name)
    existing_pref = _array_name_matches_element(existing_name, element_name)
    if new_pref and not existing_pref:
        return True
    if existing_pref and not new_pref:
        return False
    return new_oid < existing_oid


def _build_element_to_array_oid_map(
    *,
    types: Dict[int, TypeRow],
    namespaces: Dict[int, NamespaceRow],
    config: TypesConfig,
    type_name_by_oid: Dict[int, str],
) -> Dict[int, int]:
    mapping: Dict[int, int] = {}
    if not config.include_arrays:
        return mapping

    type_name_ns: Dict[Tuple[str, int], int] = {}
    for oid, row in types.items():
        if not row.typname:
            continue
        key = (row.typname, row.typnamespace)
        existing = type_name_ns.get(key)
        if existing is None or oid < existing:
            type_name_ns[key] = oid

    for arr in sorted(types.values(), key=lambda row: row.oid):
        if not is_array_type(arr):
            continue
        if not is_system_oid(arr.oid):
            continue
        namespace_name = resolve_namespace_name(namespaces, arr.typnamespace)
        if not config.include_non_pg_catalog and namespace_name != "pg_catalog":
            continue
        elem_oid = arr.typelem or 0
        candidate_name = arr.typname.lstrip("_") if arr.typname else ""
        if elem_oid and is_system_oid(elem_oid):
            existing = mapping.get(elem_oid)
            element_name = type_name_by_oid.get(elem_oid, "") or ""
            if existing is None or _should_replace_array(existing, arr.oid, element_name, types):
                mapping[elem_oid] = arr.oid
            if candidate_name and candidate_name.lower() != element_name.lower():
                candidate = type_name_ns.get((candidate_name, arr.typnamespace))
                if candidate and is_system_oid(candidate):
                    fallback = mapping.get(candidate)
                    if fallback is None or arr.oid < fallback:
                        mapping[candidate] = arr.oid
            continue
        if candidate_name:
            candidate = type_name_ns.get((candidate_name, arr.typnamespace))
            if candidate and is_system_oid(candidate):
                existing = mapping.get(candidate)
                if existing is None or arr.oid < existing:
                    mapping[candidate] = arr.oid
    return mapping


# -----------------------------------------------------------------------------
# pg_type function OID resolution (typinput/typoutput/typreceive/typsend/typmodin/typmodout)
# -----------------------------------------------------------------------------

def _normalize_regproc_key(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip()
    if s == "" or s.lower() == "null" or s == "-":
        return None
    return s


def _resolve_proc_ref_to_oid(
    *,
    raw: Optional[str],
    proc_name_to_oid: Dict[str, int],
    procs_by_oid: Dict[int, ProcRow],
    namespaces_by_oid: Dict[int, NamespaceRow],
) -> Optional[int]:
    """
    Resolve pg_proc references that may appear in pg_type exports.

    Exporters may emit:
      - numeric OID (e.g. "1247")
      - regproc name (e.g. "boolin", "pg_catalog.boolin", "\"pg_catalog\".\"boolin\"")
      - "-" / empty / null

    Strategy:
      1) numeric OID -> accept if system oid
      2) try proc_name_to_oid map (already prefers pg_catalog, then lowest oid)
      3) last resort: deterministic scan of system procs for a bare name
         (prefer pg_catalog, then lowest oid)
    """
    s = _normalize_regproc_key(raw)
    if s is None:
        return None

    # Fast path: numeric OID
    try:
        oid_val = int(float(s)) if "." in s else int(s)
        return oid_val if is_system_oid(oid_val) else None
    except ValueError:
        pass

    # Map path (keys are lowercased/normalized by build_proc_name_to_oid_map)
    best: Optional[int] = None
    for k in normalize_regproc(s):
        oid_val = proc_name_to_oid.get(k.strip().lower())
        if oid_val is not None and is_system_oid(oid_val):
            if best is None or oid_val < best:
                best = oid_val
    if best is not None:
        return best

    # Last resort: scan by bare name (stable)
    bare_keys = normalize_regproc(s)
    bare = None
    for k in bare_keys:
        if "." not in k and not k.startswith('"'):
            bare = k
            break
    if not bare:
        # maybe it only came as schema-qualified; nothing else to do
        return None

    candidates_pg: List[int] = []
    candidates_other: List[int] = []
    for oid, p in procs_by_oid.items():
        if not is_system_oid(p.oid):
            continue
        if (p.proname or "").strip().lower() != bare:
            continue
        schema = resolve_namespace_name(namespaces_by_oid, p.pronamespace or 0).lower()
        if schema == "pg_catalog":
            candidates_pg.append(oid)
        else:
            candidates_other.append(oid)

    if candidates_pg:
        return min(candidates_pg)
    if candidates_other:
        return min(candidates_other)
    return None


# -----------------------------------------------------------------------------
# Emission
# -----------------------------------------------------------------------------

def _emit_type_block(
    *,
    t: TypeRow,
    namespace_name: str,
    type_name_by_oid: Dict[int, str],
    element_to_array_oid: Dict[int, int],
    planning: Optional[dict],
    proc_name_to_oid: Dict[str, int],
    procs_by_oid: Dict[int, ProcRow],
    namespaces_by_oid: Dict[int, NamespaceRow],
) -> str:
    type_name = t.typname
    category = type_category_code(t)

    is_arr = is_array_type(t)
    element_oid = t.typelem if is_arr else None
    element_name = type_name_by_oid.get(element_oid or 0) if element_oid else None

    out: List[str] = []
    out.append("types {")
    out.append(f'  name {{ name: "{pb_escape(type_name)}" path: "{pb_escape(namespace_name)}" }}')
    out.append(f'  category: "{pb_escape(category)}"')

    if is_arr:
        out.append("  is_array: true")
        if element_name:
            out.append(
                f'  element_type {{ name: "{pb_escape(element_name)}" path: "{pb_escape(namespace_name)}" }}'
            )

    # Engine-specific: raw pg_type
    out.append("  engine_specific {")
    out.append('    payload_type: "floe.type+proto"')
    out.append("    [floe.ext.floe_type] {")
    out.append(f"      oid: {t.oid}")
    out.append(f'      typname: "{pb_escape(t.typname)}"')
    out.append(f"      typnamespace: {t.typnamespace}")
    out.append(f'      typcategory: "{pb_escape(t.typcategory)}"')
    out.append(f'      typispreferred: {"true" if t.typispreferred else "false"}')
    
    if t.typlen is not None:
        out.append(f"      typlen: {t.typlen}")
    if t.typbyval is not None:
        out.append(f"      typbyval: {'true' if t.typbyval else 'false'}")
    if t.typdelim is not None:
        out.append(f'      typdelim: "{pb_escape(t.typdelim)}"')
    if t.typalign is not None:
        out.append(f'      typalign: "{pb_escape(t.typalign)}"')

    # Array linkage
    if is_arr and t.typelem is not None and t.typelem != 0:
        out.append(f"      typelem: {t.typelem}")
    if not is_arr:
        typarray_oid = t.typarray or 0
        if not typarray_oid:
            typarray_oid = element_to_array_oid.get(t.oid) or 0
        if typarray_oid and is_system_oid(typarray_oid):
            out.append(f"      typarray: {typarray_oid}")

    if t.typcollation is not None and t.typcollation != 0:
        out.append(f"      typcollation: {t.typcollation}")

    def emit_proc_oid(field_name: str, raw_value: Optional[str]) -> None:
        oid_val = _resolve_proc_ref_to_oid(
            raw=raw_value,
            proc_name_to_oid=proc_name_to_oid,
            procs_by_oid=procs_by_oid,
            namespaces_by_oid=namespaces_by_oid,
        )
        if oid_val is not None:
            out.append(f"      {field_name}: {oid_val}")

    emit_proc_oid("typinput", getattr(t, "typinput_raw", None))
    emit_proc_oid("typoutput", getattr(t, "typoutput_raw", None))
    emit_proc_oid("typreceive", getattr(t, "typreceive_raw", None))
    emit_proc_oid("typsend", getattr(t, "typsend_raw", None))
    emit_proc_oid("typmodin", getattr(t, "typmodin_raw", None))
    emit_proc_oid("typmodout", getattr(t, "typmodout_raw", None))

    out.append("    }")
    out.append("  }")

    # Engine-specific: planning semantics (best-effort)
    if planning:
        out.append("  engine_specific {")
        out.append('    payload_type: "floe.type.planning_semantics+proto"')
        out.append("    [floe.ext.floe_type_planning_semantics] {")
        out.append(f"      type_oid: {t.oid}")

        if planning.get("eq_op_oid"):
            out.append(f"      eq_op_oid: {planning['eq_op_oid']}")
        if planning.get("btree_cmp_proc_oid"):
            out.append(f"      btree_cmp_proc_oid: {planning['btree_cmp_proc_oid']}")
        if planning.get("btree_lt_op_oid"):
            out.append(f"      btree_lt_op_oid: {planning['btree_lt_op_oid']}")
        if planning.get("btree_gt_op_oid"):
            out.append(f"      btree_gt_op_oid: {planning['btree_gt_op_oid']}")
        if planning.get("hash_proc_oid"):
            out.append(f"      hash_proc_oid: {planning['hash_proc_oid']}")
        if planning.get("default_collation_oid"):
            out.append(f"      default_collation_oid: {planning['default_collation_oid']}")
        if planning.get("btree_opfamily_oid"):
            out.append(f"      btree_opfamily_oid: {planning['btree_opfamily_oid']}")
        if planning.get("btree_opclass_oid"):
            out.append(f"      btree_opclass_oid: {planning['btree_opclass_oid']}")
        if planning.get("hash_opfamily_oid"):
            out.append(f"      hash_opfamily_oid: {planning['hash_opfamily_oid']}")
        if planning.get("hash_opclass_oid"):
            out.append(f"      hash_opclass_oid: {planning['hash_opclass_oid']}")
        out.append("    }")
        out.append("  }")

    out.append("}")
    return "\n".join(out)


def _find_am_oid(am_by_oid: Dict[int, AmRow], name: str) -> Optional[int]:
    for oid in sorted(am_by_oid.keys()):
        r = am_by_oid[oid]
        if not is_system_oid(r.oid):
            continue
        if (r.amname or "") == name:
            return r.oid
    return None


def _pick_default_opclass(
    opclasses_by_oid: Dict[int, OpclassRow],
    *,
    am_oid: int,
    type_oid: int,
) -> Optional[OpclassRow]:
    candidates: List[OpclassRow] = []
    defaults: List[OpclassRow] = []
    for oid in sorted(opclasses_by_oid.keys()):
        oc = opclasses_by_oid[oid]
        if not is_system_oid(oc.oid):
            continue
        if oc.opcmethod != am_oid:
            continue
        if oc.opcintype != type_oid:
            continue
        candidates.append(oc)
        if oc.opcdefault:
            defaults.append(oc)
    if defaults:
        return defaults[0]
    return candidates[0] if candidates else None


def _find_amop_operator(
    amops: List[AmopRow],
    *,
    family_oid: int,
    strategy: int,
    type_oid: int,
    require_purpose: Optional[str] = "s",
) -> Optional[int]:
    for r in amops:
        if r.amopfamily != family_oid:
            continue
        if r.amopstrategy != strategy:
            continue
        if (r.amoplefttype or 0) != type_oid:
            continue
        if (r.amoprighttype or 0) != type_oid:
            continue
        if require_purpose is not None and (r.amoppurpose or "") not in ("", require_purpose):
            continue
        if r.amopopr and is_system_oid(r.amopopr):
            return r.amopopr
    return None


def _find_amproc_function(
    amprocs,
    *,
    family_oid: int,
    procnum: int,
    type_oid: int,
    proc_name_to_oid: Dict[str, int],
) -> Optional[int]:
    """
    Find a support procedure function OID from pg_amproc for a given
    (family, procnum, type).

    Handles exporters that emit pg_amproc.amproc as either:
      - numeric OID
      - regproc name (e.g. "btint4cmp")
    """
    for r in amprocs:
        if (r.amprocfamily or 0) != family_oid:
            continue
        if (r.amprocnum or 0) != procnum:
            continue
        if (r.amproclefttype or 0) != type_oid and (r.amprocrighttype or 0) != type_oid:
            continue

        func_oid: Optional[int] = None

        if hasattr(r, "amproc"):
            func_oid = getattr(r, "amproc")

        if func_oid is None:
            func_oid = getattr(r, "amproc_oid", None)

        if func_oid is None:
            amproc_name = getattr(r, "amproc_name", None)
            if amproc_name:
                func_oid = proc_name_to_oid.get(amproc_name)

        if func_oid is not None and is_system_oid(func_oid):
            return func_oid

    return None


def _compute_planning_semantics(
    *,
    t: TypeRow,
    am_by_oid: Dict[int, AmRow],
    opclass_by_oid: Dict[int, OpclassRow],
    amops: List[AmopRow],
    amprocs: List[AmprocRow],
    proc_name_to_oid: Dict[str, int],
) -> Optional[dict]:
    out: dict = {}

    if t.typcollation and t.typcollation != 0:
        out["default_collation_oid"] = t.typcollation

    btree_oid = _find_am_oid(am_by_oid, "btree")
    hash_oid = _find_am_oid(am_by_oid, "hash")

    if btree_oid:
        btree_oc = _pick_default_opclass(opclass_by_oid, am_oid=btree_oid, type_oid=t.oid)
        if btree_oc:
            out["btree_opclass_oid"] = btree_oc.oid
            out["btree_opfamily_oid"] = btree_oc.opcfamily

            eq = _find_amop_operator(amops, family_oid=btree_oc.opcfamily, strategy=3, type_oid=t.oid)
            lt = _find_amop_operator(amops, family_oid=btree_oc.opcfamily, strategy=1, type_oid=t.oid)
            gt = _find_amop_operator(amops, family_oid=btree_oc.opcfamily, strategy=5, type_oid=t.oid)
            cmp_fn = _find_amproc_function(
                amprocs,
                family_oid=btree_oc.opcfamily,
                procnum=1,
                type_oid=t.oid,
                proc_name_to_oid=proc_name_to_oid,
            )

            if eq:
                out["eq_op_oid"] = eq
            if lt:
                out["btree_lt_op_oid"] = lt
            if gt:
                out["btree_gt_op_oid"] = gt
            if cmp_fn:
                out["btree_cmp_proc_oid"] = cmp_fn

    if hash_oid:
        hash_oc = _pick_default_opclass(opclass_by_oid, am_oid=hash_oid, type_oid=t.oid)
        if hash_oc:
            out["hash_opclass_oid"] = hash_oc.oid
            out["hash_opfamily_oid"] = hash_oc.opcfamily

            hash_fn = _find_amproc_function(
                amprocs,
                family_oid=hash_oc.opcfamily,
                procnum=1,
                type_oid=t.oid,
                proc_name_to_oid=proc_name_to_oid,
            )
            if hash_fn:
                out["hash_proc_oid"] = hash_fn

    return out if out else None


def generate_types_pbtxt(
    *,
    csv_dir: Path,
    config: TypesConfig,
    diagnostics: Diagnostics | None = None,
) -> tuple[str, int]:
    diagnostics = diagnostics or Diagnostics()
    stats = diagnostics.get_module_stats("types")
    namespaces = read_namespaces(csv_dir)
    types = read_types(csv_dir)
    procs_by_oid = read_procs(csv_dir)
    proc_name_to_oid = build_proc_name_to_oid_map(procs_by_oid, namespaces)

    am_by_oid = read_am(csv_dir)
    opclass_by_oid = read_opclass(csv_dir)
    amops = read_amop(csv_dir)
    amprocs = read_amproc(csv_dir)

    type_name_by_oid: Dict[int, str] = {oid: t.typname for oid, t in types.items() if t.typname}

    required_type_oids = _collect_required_type_oids(procs_by_oid)
    element_to_array_oid = _build_element_to_array_oid_map(
        types=types,
        namespaces=namespaces,
        config=config,
        type_name_by_oid=type_name_by_oid,
    )

    blocks: List[str] = []

    for oid in sorted(types.keys()):
        t = types[oid]

        stats.record_read()

        if not is_system_oid(t.oid):
            stats.record_drop(DropReason.DROP_NOT_SYSTEM_OID, f"oid={t.oid}")
            continue
        stats.record_system_pass()

        is_table_row = is_table_row_type(t)
        if is_table_row and not config.include_table_rowtypes and t.oid not in required_type_oids:
            continue
        if not config.include_arrays and is_array_type(t):
            continue

        namespace_name = resolve_namespace_name(namespaces, t.typnamespace)
        if not config.include_non_pg_catalog and namespace_name != "pg_catalog":
            stats.record_drop(
                DropReason.DROP_NAMESPACE_FILTER,
                f"type={t.typname} namespace={namespace_name}",
            )
            continue

        planning = _compute_planning_semantics(
            t=t,
            am_by_oid=am_by_oid,
            opclass_by_oid=opclass_by_oid,
            amops=amops,
            amprocs=amprocs,
            proc_name_to_oid=proc_name_to_oid,
        )

        blocks.append(
            _emit_type_block(
                t=t,
                namespace_name=namespace_name,
                type_name_by_oid=type_name_by_oid,
                element_to_array_oid=element_to_array_oid,
                planning=planning,
                proc_name_to_oid=proc_name_to_oid,
                procs_by_oid=procs_by_oid,
                namespaces_by_oid=namespaces,
            )
        )
        stats.record_emitted()

    content = "\n\n".join(blocks) + ("\n" if blocks else "")
    content += f"# types count: {len(blocks)}\n"
    return with_pbtxt_header(content), len(blocks)


def write_types_pbtxt(
    *,
    csv_dir: Path,
    out_path: Path,
    config: TypesConfig,
    diagnostics: Diagnostics | None = None,
) -> int:
    content, count = generate_types_pbtxt(
        csv_dir=csv_dir,
        config=config,
        diagnostics=diagnostics,
    )
    write_text(out_path, content)
    return count


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 types_pbtxt.py <csv_dir> <out_types_pbtxt>", file=sys.stderr)
        raise SystemExit(2)

    csv_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    write_types_pbtxt(csv_dir=csv_dir, out_path=out_path, config=TypesConfig())
    print(f"Wrote {out_path}")