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
FloeDB catalog importer (CSV -> pbtxt).

Reads exported Postgres catalog CSVs (from your earlier export script),
and emits Floecat pbtxt files for each object type.

Right now:
- 10_types.pbtxt  (from pg_type.csv + pg_namespace.csv)
- 20_functions.pbtxt  (pg_proc)
- 30_operators.pbtxt  (pg_operator)
- 40_casts.pbtxt      (pg_cast)
- 50_collations.pbtxt (pg_collation)
- 60_aggregates.pbtxt (pg_aggregate)
- 00_registry.pbtxt   (pg_am, pg_opfamily, pg_opclass, pg_amop, pg_amproc)
"""

from __future__ import annotations

import argparse
from pathlib import Path

from diagnostics import Diagnostics
from types_pbtxt import TypesConfig, write_types_pbtxt
from functions_pbtxt import FunctionsConfig, write_functions_pbtxt
from operators_pbtxt import OperatorsConfig, write_operators_pbtxt
from casts_pbtxt import CastsConfig, write_casts_pbtxt
from collations_pbtxt import CollationsConfig, write_collations_pbtxt
from aggregates_pbtxt import AggregatesConfig, write_aggregates_pbtxt
from registries_pbtxt import RegistriesConfig, write_registries_pbtxt

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import PG catalog CSVs into Floecat pbtxt files.")
    p.add_argument(
        "--csv-dir",
        type=Path,
        required=True,
        help="Directory containing pg_*.csv exports (pg_type.csv, pg_namespace.csv, ...)",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory to write generated pbtxt files into",
    )

    # Shared policy knobs (system-only is always enforced for now).
    p.add_argument(
        "--pg-catalog-only",
        action="store_true",
        default=False,
        help="Keep only objects in namespace pg_catalog (instead of all system namespaces).",
    )
    p.add_argument(
        "--include-table-rowtypes",
        action="store_true",
        default=False,
        help="Include composite table row types (typtype='c' with typrelid != 0). Default: off.",
    )
    p.add_argument(
        "--no-arrays",
        action="store_true",
        default=False,
        help="Exclude array types (typcategory='A' or typname starting with '_').",
    )
    
    p.add_argument(
        "--include-proc-sources",
        action="store_true",
        default=False,
        help="Include pg_proc.prosrc in engine_specific payloads (debug only). Default: off.",
    )
    p.add_argument(
        "--include-variadic",
        action="store_true",
        default=False,
        help="Include variadic argument element types when present. Default: off.",
    )
    p.add_argument(
        "--stats",
        action="store_true",
        default=False,
        help="Print per-module row processing statistics.",
    )
    p.add_argument(
        "--debug-drops",
        type=int,
        default=0,
        metavar="N",
        help="Log the first N dropped rows (reason codes).",
    )
    
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_dir: Path = args.csv_dir
    out_dir: Path = args.out_dir

    out_dir.mkdir(parents=True, exist_ok=True)

    diagnostics = Diagnostics(
        stats_enabled=args.stats,
        debug_drops_limit=args.debug_drops,
    )

    # ---- TYPES ----
    types_cfg = TypesConfig(
        include_non_pg_catalog=(not args.pg_catalog_only),
        include_table_rowtypes=args.include_table_rowtypes,
        include_arrays=(not args.no_arrays),
    )

    out_types = out_dir / "10_types.pbtxt"
    type_count = write_types_pbtxt(
        csv_dir=csv_dir,
        out_path=out_types,
        config=types_cfg,
        diagnostics=diagnostics,
    )
    print(f"[OK] wrote {out_types} ({type_count} types)")
  
    # ---- FUNCTIONS ----
    funcs_cfg = FunctionsConfig(
        include_non_pg_catalog=(not args.pg_catalog_only),
        include_prosrc=args.include_proc_sources,
        include_variadic=args.include_variadic,
    )

    out_funcs = out_dir / "20_functions.pbtxt"
    func_count = write_functions_pbtxt(
        csv_dir=csv_dir,
        out_path=out_funcs,
        config=funcs_cfg,
        diagnostics=diagnostics,
    )
    print(f"[OK] wrote {out_funcs} ({func_count} functions)")
    
    # ---- OPERATORS ----
    ops_cfg = OperatorsConfig(
        include_non_pg_catalog=(not args.pg_catalog_only),
    )

    out_ops = out_dir / "30_operators.pbtxt"
    op_count = write_operators_pbtxt(
        csv_dir=csv_dir,
        out_path=out_ops,
        config=ops_cfg,
        diagnostics=diagnostics,
    )
    print(f"[OK] wrote {out_ops} ({op_count} operators)")

    # ---- CASTS ----
    casts_cfg = CastsConfig(
        include_non_pg_catalog=(not args.pg_catalog_only),
    )

    out_casts = out_dir / "40_casts.pbtxt"
    cast_count = write_casts_pbtxt(
        csv_dir=csv_dir,
        out_path=out_casts,
        config=casts_cfg,
        diagnostics=diagnostics,
    )
    print(f"[OK] wrote {out_casts} ({cast_count} casts)")

    # ---- COLLATIONS ----
    colls_cfg = CollationsConfig(
        include_non_pg_catalog=(not args.pg_catalog_only),
    )

    out_colls = out_dir / "50_collations.pbtxt"
    coll_count = write_collations_pbtxt(
        csv_dir=csv_dir,
        out_path=out_colls,
        config=colls_cfg,
        diagnostics=diagnostics,
    )
    print(f"[OK] wrote {out_colls} ({coll_count} collations)")
    
    # ---- AGGREGATES ----
    aggs_cfg = AggregatesConfig(
        include_non_pg_catalog=(not args.pg_catalog_only),
    )

    out_aggs = out_dir / "60_aggregates.pbtxt"
    agg_count = write_aggregates_pbtxt(
        csv_dir=csv_dir,
        out_path=out_aggs,
        config=aggs_cfg,
        diagnostics=diagnostics,
    )
    print(f"[OK] wrote {out_aggs} ({agg_count} aggregates)")
    
    # ---- REGISTRIES ----
    regs_cfg = RegistriesConfig()
    out_regs = out_dir / "00_registry.pbtxt"
    registry_counts = write_registries_pbtxt(
        csv_dir=csv_dir,
        out_path=out_regs,
        config=regs_cfg,
        diagnostics=diagnostics,
    )
    registry_summary = ", ".join(f"{k}={v}" for k, v in registry_counts.items())
    print(f"[OK] wrote {out_regs} ({registry_summary})")
    if args.stats:
        for line in diagnostics.summary_lines():
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
