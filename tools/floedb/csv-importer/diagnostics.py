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
"""Shared diagnostics for CSV -> PBtxt generators."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List


class DropReason(Enum):
    DROP_NOT_SYSTEM_OID = "DROP_NOT_SYSTEM_OID"
    DROP_NAMESPACE_FILTER = "DROP_NAMESPACE_FILTER"
    DROP_MISSING_TYPE = "DROP_MISSING_TYPE"
    DROP_MISSING_PG_PROC = "DROP_MISSING_PG_PROC"
    DROP_UNRESOLVED_REGPROC = "DROP_UNRESOLVED_REGPROC"
    DROP_MISSING_DEP = "DROP_MISSING_DEP"
    DROP_DUPLICATE_SIGNATURE = "DROP_DUPLICATE_SIGNATURE"


class DropLogger:
    def __init__(self, limit: int = 0) -> None:
        self.limit = max(0, limit)
        self.count = 0

    def log(self, module: str, reason: DropReason, detail: str) -> None:
        if self.limit <= 0 or self.count >= self.limit:
            return
        print(f"[DROP][{module}][{reason.value}] {detail}")
        self.count += 1


@dataclass
class ModuleStats:
    module_name: str
    diagnostics: Diagnostics  # type: ignore[name-defined]
    rows_read: int = 0
    rows_after_system: int = 0
    rows_dropped_missing_dep: int = 0
    rows_dropped_namespace: int = 0
    rows_emitted: int = 0

    def record_read(self) -> None:
        self.rows_read += 1

    def record_system_pass(self) -> None:
        self.rows_after_system += 1

    def record_drop(self, reason: DropReason, detail: str) -> None:
        if reason == DropReason.DROP_NAMESPACE_FILTER:
            self.rows_dropped_namespace += 1
        elif reason in (
            DropReason.DROP_MISSING_TYPE,
            DropReason.DROP_MISSING_PG_PROC,
            DropReason.DROP_UNRESOLVED_REGPROC,
            DropReason.DROP_MISSING_DEP,
            DropReason.DROP_DUPLICATE_SIGNATURE,
        ):
            self.rows_dropped_missing_dep += 1
        self.diagnostics.log_drop(self.module_name, reason, detail)

    def record_emitted(self) -> None:
        self.rows_emitted += 1


class Diagnostics:
    def __init__(self, stats_enabled: bool = False, debug_drops_limit: int = 0) -> None:
        self.stats_enabled = stats_enabled
        self.drop_logger = DropLogger(debug_drops_limit)
        self._modules: Dict[str, ModuleStats] = {}

    def get_module_stats(self, module_name: str) -> ModuleStats:
        if module_name not in self._modules:
            self._modules[module_name] = ModuleStats(module_name, diagnostics=self)
        return self._modules[module_name]

    def log_drop(self, module: str, reason: DropReason, detail: str) -> None:
        self.drop_logger.log(module, reason, detail)

    def summary_lines(self) -> List[str]:
        if not self.stats_enabled:
            return []
        lines: List[str] = []
        for stats in sorted(self._modules.values(), key=lambda s: s.module_name):
            lines.append(f"Module {stats.module_name}:")
            lines.append(f"  rows_read: {stats.rows_read}")
            lines.append(f"  rows_after_system_filter: {stats.rows_after_system}")
            lines.append(f"  rows_dropped_missing_deps: {stats.rows_dropped_missing_dep}")
            lines.append(f"  rows_dropped_namespace: {stats.rows_dropped_namespace}")
            lines.append(f"  rows_emitted: {stats.rows_emitted}")
        return lines
