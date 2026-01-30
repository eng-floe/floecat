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
Shared utilities for CSV -> PBtxt generators for FloeCat system catalog objects.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


SYSTEM_OID_CEILING = 16384  # system objects: OID < 16384


def is_system_oid(oid: Optional[int]) -> bool:
    return oid is not None and oid < SYSTEM_OID_CEILING


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def pick_oid_field(row: Dict[str, str]) -> str:
    if "oid" in row:
        return "oid"
    for k in ("oid_1", "OID", "Oid"):
        if k in row:
            return k
    for k in row.keys():
        if k.lower() == "oid":
            return k
    raise KeyError(f"No oid column found. Headers: {sorted(row.keys())}")


def to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return None
    try:
        if "." in s:
            return int(float(s))
        return int(s)
    except ValueError:
        return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def to_bool_pg(v: Any) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return None
    if s in ("t", "true", "True", "1"):
        return True
    if s in ("f", "false", "False", "0"):
        return False
    return None


def to_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if s.strip() == "" or s.strip().lower() == "null" or s.strip() == "-":
        return None
    return s

def to_symbol_str(v: Any) -> Optional[str]:
    """Like to_str, but '-' is a legitimate value (e.g. operator name)."""
    if v is None:
        return None
    s = str(v)
    if s.strip() == "" or s.strip().lower() == "null":
        return None
    return s

def pb_escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


PBTXT_LEGAL_HEADER = """# Copyright 2026 Yellowbrick Data, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""


def with_pbtxt_header(content: str) -> str:
    if content.startswith("# Copyright 2026 Yellowbrick Data, Inc"):
        return content
    return f"{PBTXT_LEGAL_HEADER}{content}"


def parse_oidvector(s: Any) -> List[int]:
    if s is None:
        return []
    txt = str(s).strip()
    if txt == "" or txt.lower() == "null":
        return []
    out: List[int] = []
    for part in txt.split():
        oid = to_int(part)
        if oid is not None:
            out.append(oid)
    return out


# =============================================================================
# pg_namespace
# =============================================================================

@dataclass(frozen=True)
class NamespaceRow:
    oid: int
    nspname: str


def read_namespaces(csv_dir: Path) -> Dict[int, NamespaceRow]:
    path = csv_dir / "pg_namespace.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    out: Dict[int, NamespaceRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        nspname = to_str(r.get("nspname"))
        if oid is None or nspname is None:
            continue
        out[oid] = NamespaceRow(oid=oid, nspname=nspname)
    return out


def resolve_namespace_name(namespaces: Dict[int, NamespaceRow], oid: int) -> str:
    return namespaces.get(oid, NamespaceRow(oid=oid, nspname="pg_catalog")).nspname



# =============================================================================
# pg_type
# =============================================================================

@dataclass(frozen=True)
class TypeRow:
    oid: int
    typname: str
    typnamespace: int

    typlen: Optional[int]
    typbyval: Optional[bool]
    typdelim: Optional[str]
    typalign: Optional[str]

    typelem: Optional[int]
    typarray: Optional[int]

    typtype: Optional[str]
    typrelid: Optional[int]

    typcollation: Optional[int]
    
    typcategory: Optional[str]
    typispreferred: Optional[bool]

    typinput_raw: Optional[str]
    typoutput_raw: Optional[str]
    typreceive_raw: Optional[str]
    typsend_raw: Optional[str]
    typmodin_raw: Optional[str]
    typmodout_raw: Optional[str]


def read_types(csv_dir: Path) -> Dict[int, TypeRow]:
    path = csv_dir / "pg_type.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    out: Dict[int, TypeRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue
        out[oid] = TypeRow(
            oid=oid,
            typname=to_str(r.get("typname")) or "",
            typnamespace=to_int(r.get("typnamespace")) or 0,
            typlen=to_int(r.get("typlen")),
            typbyval=to_bool_pg(r.get("typbyval")),
            typdelim=to_str(r.get("typdelim")),
            typalign=to_str(r.get("typalign")),
            typelem=to_int(r.get("typelem")),
            typarray=to_int(r.get("typarray")),
            typtype=to_str(r.get("typtype")),
            typrelid=to_int(r.get("typrelid")),
            typcollation=to_int(r.get("typcollation")),
            typcategory=to_str(r.get("typcategory")),
            typispreferred=to_bool_pg(r.get("typispreferred")),
            typinput_raw=to_str(r.get("typinput")),
            typoutput_raw=to_str(r.get("typoutput")),
            typreceive_raw=to_str(r.get("typreceive")),
            typsend_raw=to_str(r.get("typsend")),
            typmodin_raw=to_str(r.get("typmodin")),
            typmodout_raw=to_str(r.get("typmodout")),
        )
    return out


def is_array_type(t: TypeRow) -> bool:
    return (t.typelem or 0) != 0 and t.typname.startswith("_")


def is_table_row_type(t: TypeRow) -> bool:
    return (t.typtype == "c") and ((t.typrelid or 0) != 0)


def type_category_code(t: TypeRow) -> str:
    name = t.typname.lower()
    if is_array_type(t):
        return "A"
    if name in ("bool",):
        return "B"
    if name in ("int2", "int4", "int8", "oid", "xid", "cid", "float4", "float8", "numeric", "money"):
        return "N"
    if name in ("text", "varchar", "bpchar", "name", "char"):
        return "S"
    if name in ("date", "time", "timetz", "timestamp", "timestamptz", "interval"):
        return "D"
    return "U"



# =============================================================================
# pg_proc loader
# =============================================================================
@dataclass(frozen=True)
class ProcRow:
    oid: int
    proname: str

    pronamespace: Optional[int]
    proowner: Optional[int]
    prolang: Optional[int]

    procost: Optional[float]
    prorows: Optional[float]

    provariadic: Optional[int]
    protransform: Optional[int]

    proisagg: bool
    proiswindow: bool
    prosecdef: Optional[bool]
    proleakproof: Optional[bool]
    proisstrict: Optional[bool]
    proretset: Optional[bool]
    provolatile: Optional[str]

    pronargs: Optional[int]
    pronargdefaults: Optional[int]

    prorettype: int
    proargtypes: List[int]

    proybcost: Optional[int]

    prosrc: Optional[str]


def read_procs(csv_dir: Path) -> Dict[int, ProcRow]:
    path = csv_dir / "pg_proc.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")

    out: Dict[int, ProcRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue

        proname = to_str(r.get("proname")) or ""

        proisagg = bool(to_bool_pg(r.get("proisagg")) or False)
        proiswindow = bool(to_bool_pg(r.get("proiswindow")) or False)

        out[oid] = ProcRow(
            oid=oid,
            proname=proname,
            pronamespace=to_int(r.get("pronamespace")),
            proowner=to_int(r.get("proowner")),
            prolang=to_int(r.get("prolang")),
            procost=_to_float(r.get("procost")),
            prorows=_to_float(r.get("prorows")),
            provariadic=to_int(r.get("provariadic")),
            protransform=to_int(r.get("protransform")),
            proisagg=proisagg,
            proiswindow=proiswindow,
            prosecdef=to_bool_pg(r.get("prosecdef")),
            proleakproof=to_bool_pg(r.get("proleakproof")),
            proisstrict=to_bool_pg(r.get("proisstrict")),
            proretset=to_bool_pg(r.get("proretset")),
            provolatile=to_str(r.get("provolatile")),
            pronargs=to_int(r.get("pronargs")),
            pronargdefaults=to_int(r.get("pronargdefaults")),
            prorettype=to_int(r.get("prorettype")) or 0,
            proargtypes=parse_oidvector(r.get("proargtypes")),
            proybcost=to_int(r.get("proybcost")),
            prosrc=to_str(r.get("prosrc")),
        )

    return out


def build_proc_name_to_oid_map(
    procs_by_oid: Dict[int, ProcRow],
    namespaces_by_oid: Dict[int, NamespaceRow],
) -> Dict[str, int]:
    """
    Build a best-effort regproc name -> oid map using system-only pg_proc rows.

    Prefers entries in pg_catalog when duplicates exist.
    """
    name_to_oid: Dict[str, int] = {}

    def add_candidate(key: str, oid_val: int) -> None:
        normalized = key.strip().lower()
        if not normalized:
            return
        existing = name_to_oid.get(normalized)
        if existing is None or oid_val < existing:
            name_to_oid[normalized] = oid_val

    for oid, proc in procs_by_oid.items():
        if not is_system_oid(proc.oid):
            continue
        if not proc.proname:
            continue

        schema_name = resolve_namespace_name(namespaces_by_oid, proc.pronamespace or 0)
        candidates: Set[str] = set()
        candidates.update(normalize_regproc(proc.proname))
        if schema_name:
            candidates.update(normalize_regproc(f"{schema_name}.{proc.proname}"))
            candidates.update(normalize_regproc(f'"{schema_name}"."{proc.proname}"'))

        for candidate in candidates:
            add_candidate(candidate, oid)

    return name_to_oid


def normalize_regproc(raw: Optional[str]) -> Set[str]:
    """Normalize regproc-like strings into lookup keys."""
    if not raw:
        return set()
    text = raw.strip()
    if not text:
        return set()
    if "(" in text:
        text = text[: text.index("(")].strip()

    parts: List[str] = []
    buf = ""
    in_quote = False
    for ch in text:
        if ch == '"':
            in_quote = not in_quote
            buf += ch
        elif ch == "." and not in_quote:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    if buf:
        parts.append(buf)

    cleaned = [p.strip() for p in parts if p.strip()]
    if not cleaned:
        return set()

    def dequote(value: str) -> str:
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        return value

    base = dequote(cleaned[-1]).lower()
    keys: Set[str] = {base}
    if len(cleaned) >= 2:
        schema = dequote(cleaned[-2]).lower()
        keys.add(f"{schema}.{base}")
        keys.add(f'"{schema}"."{base}"')
    return keys


# =============================================================================
# pg_operator loader
# =============================================================================

@dataclass(frozen=True)
class OperatorRow:
    oid: int
    oprname: str
    oprnamespace: Optional[int]

    oprkind: Optional[str]
    oprcanmerge: Optional[bool]
    oprcanhash: Optional[bool]

    oprleft: Optional[int]
    oprright: Optional[int]
    oprresult: Optional[int]
    oprcom: Optional[int]
    oprnegate: Optional[int]

    oprcode_oid: Optional[int]
    oprrest_oid: Optional[int]
    oprjoin_oid: Optional[int]

    oprcode_raw: Optional[str]
    oprrest_raw: Optional[str]
    oprjoin_raw: Optional[str]


def read_operators(csv_dir: Path) -> Dict[int, OperatorRow]:
    path = csv_dir / "pg_operator.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")

    out: Dict[int, OperatorRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue

        # These three are regproc-like in PG; exporters may emit numeric OIDs or names.
        oprcode_raw = to_str(r.get("oprcode"))
        oprrest_raw = to_str(r.get("oprrest"))
        oprjoin_raw = to_str(r.get("oprjoin"))

        oprcode_oid = to_int(oprcode_raw) if oprcode_raw is not None else None
        oprrest_oid = to_int(oprrest_raw) if oprrest_raw is not None else None
        oprjoin_oid = to_int(oprjoin_raw) if oprjoin_raw is not None else None

        out[oid] = OperatorRow(
            oid=oid,
            oprname=to_symbol_str(r.get("oprname")) or "",
            oprnamespace=to_int(r.get("oprnamespace")),
            oprkind=to_str(r.get("oprkind")),
            oprcanmerge=to_bool_pg(r.get("oprcanmerge")),
            oprcanhash=to_bool_pg(r.get("oprcanhash")),
            oprleft=to_int(r.get("oprleft")),
            oprright=to_int(r.get("oprright")),
            oprresult=to_int(r.get("oprresult")),
            oprcom=to_int(r.get("oprcom")),
            oprnegate=to_int(r.get("oprnegate")),
            oprcode_oid=oprcode_oid,
            oprrest_oid=oprrest_oid,
            oprjoin_oid=oprjoin_oid,
            oprcode_raw=None if oprcode_oid is not None else oprcode_raw,
            oprrest_raw=None if oprrest_oid is not None else oprrest_raw,
            oprjoin_raw=None if oprjoin_oid is not None else oprjoin_raw,
        )

    return out


# =============================================================================
# pg_cast loader
# =============================================================================

@dataclass(frozen=True)
class CastRow:
    castsource: Optional[int]
    casttarget: Optional[int]
    castfunc: Optional[int]
    castcontext: Optional[str]
    castmethod: Optional[str]


def read_casts(csv_dir: Path) -> Dict[tuple[int, int, int], CastRow]:
    path = csv_dir / "pg_cast.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")

    out: Dict[tuple[int, int, int], CastRow] = {}
    for r in read_csv(path):
        castsource = to_int(r.get("castsource"))
        casttarget = to_int(r.get("casttarget"))
        castfunc = to_int(r.get("castfunc")) or 0
        if castsource is None or casttarget is None:
            continue

        out[(castsource, casttarget, castfunc)] = CastRow(
            castsource=castsource,
            casttarget=casttarget,
            castfunc=castfunc,
            castcontext=to_str(r.get("castcontext")),
            castmethod=to_str(r.get("castmethod")),
        )

    return out


# =============================================================================
# pg_collation loader
# =============================================================================

@dataclass(frozen=True)
class CollationRow:
    oid: int
    collname: str
    collnamespace: int
    collencoding: Optional[int]
    collcollate: Optional[str]
    collctype: Optional[str]


def read_collations(csv_dir: Path) -> Dict[int, CollationRow]:
    path = csv_dir / "pg_collation.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")

    out: Dict[int, CollationRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue

        out[oid] = CollationRow(
            oid=oid,
            collname=to_str(r.get("collname")) or "",
            collnamespace=to_int(r.get("collnamespace")) or 0,
            collencoding=to_int(r.get("collencoding")),
            collcollate=to_str(r.get("collcollate")),
            collctype=to_str(r.get("collctype")),
        )

    return out


# =============================================================================
# pg_aggregate loader
# =============================================================================

@dataclass(frozen=True)
class AggRow:
    aggfnoid: Optional[int]
    aggfnoid_name: Optional[str]
    aggkind: Optional[str]
    aggnumdirectargs: Optional[int]

    aggtransfn: Optional[int]
    aggfinalfn: Optional[int]
    aggmtransfn: Optional[int]
    aggminvtransfn: Optional[int]
    aggmfinalfn: Optional[int]

    aggfinalextra: Optional[bool]
    aggmfinalextra: Optional[bool]

    aggsortop: Optional[int]
    aggtranstype: Optional[int]
    aggtransspace: Optional[int]

    aggmtranstype: Optional[int]
    aggmtransspace: Optional[int]

    agginitval: Optional[str]
    aggminitval: Optional[str]


def read_aggregates(csv_dir: Path) -> List[AggRow]:
    path = csv_dir / "pg_aggregate.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")

    out: List[AggRow] = []
    for r in read_csv(path):
        aggfnoid_raw = to_str(r.get("aggfnoid"))
        aggfnoid = to_int(aggfnoid_raw)
        aggfnoid_name = aggfnoid_raw if aggfnoid is None else None

        out.append(
            AggRow(
                aggfnoid=aggfnoid,
                aggfnoid_name=aggfnoid_name,
                aggkind=to_str(r.get("aggkind")),
                aggnumdirectargs=to_int(r.get("aggnumdirectargs")),
                aggtransfn=to_int(r.get("aggtransfn")),
                aggfinalfn=to_int(r.get("aggfinalfn")),
                aggmtransfn=to_int(r.get("aggmtransfn")),
                aggminvtransfn=to_int(r.get("aggminvtransfn")),
                aggmfinalfn=to_int(r.get("aggmfinalfn")),
                aggfinalextra=to_bool_pg(r.get("aggfinalextra")),
                aggmfinalextra=to_bool_pg(r.get("aggmfinalextra")),
                aggsortop=to_int(r.get("aggsortop")),
                aggtranstype=to_int(r.get("aggtranstype")),
                aggtransspace=to_int(r.get("aggtransspace")),
                aggmtranstype=to_int(r.get("aggmtranstype")),
                aggmtransspace=to_int(r.get("aggmtransspace")),
                agginitval=to_str(r.get("agginitval")),
                aggminitval=to_str(r.get("aggminitval")),
            )
        )

    return out

# =============================================================================
# Registry tables for planning semantics + registries_pbtxt
# =============================================================================

@dataclass(frozen=True)
class AmRow:
    oid: int
    amname: Optional[str]


def read_am(csv_dir: Path) -> Dict[int, AmRow]:
    path = csv_dir / "pg_am.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    out: Dict[int, AmRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue
        out[oid] = AmRow(oid=oid, amname=to_str(r.get("amname")))
    return out


@dataclass(frozen=True)
class OpfamilyRow:
    oid: int
    opfmethod: Optional[int]
    opfname: Optional[str]
    opfnamespace: Optional[int]


def read_opfamily(csv_dir: Path) -> Dict[int, OpfamilyRow]:
    path = csv_dir / "pg_opfamily.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    out: Dict[int, OpfamilyRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue
        out[oid] = OpfamilyRow(
            oid=oid,
            opfmethod=to_int(r.get("opfmethod")),
            opfname=to_str(r.get("opfname")),
            opfnamespace=to_int(r.get("opfnamespace")),
        )
    return out


@dataclass(frozen=True)
class OpclassRow:
    oid: int
    opcmethod: int
    opcfamily: int
    opcintype: int
    opcdefault: Optional[bool]
    opcname: Optional[str]
    opcnamespace: Optional[int]


def read_opclass(csv_dir: Path) -> Dict[int, OpclassRow]:
    path = csv_dir / "pg_opclass.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    out: Dict[int, OpclassRow] = {}
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue
        out[oid] = OpclassRow(
            oid=oid,
            opcmethod=to_int(r.get("opcmethod")) or 0,
            opcfamily=to_int(r.get("opcfamily")) or 0,
            opcintype=to_int(r.get("opcintype")) or 0,
            opcdefault=to_bool_pg(r.get("opcdefault")),
            opcname=to_str(r.get("opcname")),
            opcnamespace=to_int(r.get("opcnamespace")),
        )
    return out


@dataclass(frozen=True)
class AmopRow:
    amopfamily: Optional[int]
    amopstrategy: Optional[int]
    amoplefttype: Optional[int]
    amoprighttype: Optional[int]
    amopopr: Optional[int]
    amoppurpose: Optional[str]
    amopsortfamily: Optional[int]
    amopmethod: Optional[int]


def read_amop(csv_dir: Path) -> List[AmopRow]:
    path = csv_dir / "pg_amop.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    out: List[AmopRow] = []
    for r in read_csv(path):
        out.append(
            AmopRow(
                amopfamily=to_int(r.get("amopfamily")),
                amopstrategy=to_int(r.get("amopstrategy")),
                amoplefttype=to_int(r.get("amoplefttype")),
                amoprighttype=to_int(r.get("amoprighttype")),
                amopopr=to_int(r.get("amopopr")),
                amoppurpose=to_str(r.get("amoppurpose")),
                amopsortfamily=to_int(r.get("amopsortfamily")),
                amopmethod=to_int(r.get("amopmethod")),
            )
        )
    return out


# =============================================================================
# pg_amproc loader (support procedures)
# =============================================================================

@dataclass(frozen=True)
class AmprocRow:
    oid: int
    amprocfamily: Optional[int]
    amproclefttype: Optional[int]
    amprocrighttype: Optional[int]
    amprocnum: Optional[int]

    # pg_amproc.amproc is regproc; exporters may output either numeric OID or name.
    amproc_oid: Optional[int]
    amproc_name: Optional[str]


def read_amproc(csv_dir: Path) -> List[AmprocRow]:
    path = csv_dir / "pg_amproc.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")

    out: List[AmprocRow] = []
    for r in read_csv(path):
        oid = to_int(r.get(pick_oid_field(r)))
        if oid is None:
            continue

        amproc_raw = to_str(r.get("amproc"))
        amproc_oid = to_int(amproc_raw) if amproc_raw is not None else None
        amproc_name = None if amproc_oid is not None else amproc_raw

        out.append(
            AmprocRow(
                oid=oid,
                amprocfamily=to_int(r.get("amprocfamily")),
                amproclefttype=to_int(r.get("amproclefttype")),
                amprocrighttype=to_int(r.get("amprocrighttype")),
                amprocnum=to_int(r.get("amprocnum")),
                amproc_oid=amproc_oid,
                amproc_name=amproc_name,
            )
        )
    return out
