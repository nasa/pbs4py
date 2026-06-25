"""
Queue availability via `qstat -Qf <queue>`.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass
class QueueStats:
    name: str
    total_jobs: int
    running: int
    queued: int
    ncpus_assigned: int | None
    ncpus_max: int | None

    @property
    def percent_cpus_free(self) -> float | None:
        if not self.ncpus_max or self.ncpus_assigned is None:
            return None
        return 100.0 * (1.0 - self.ncpus_assigned / self.ncpus_max)


def get_queue_stats(queue_name: str) -> QueueStats | None:
    result = subprocess.run(
        ["qstat", "-Qf", queue_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        return None

    info: dict[str, str] = {}
    current_key: str | None = None
    for line in result.stdout.decode("utf-8", errors="replace").splitlines()[1:]:
        if not line:
            continue
        if line.startswith("\t") and current_key is not None:
            info[current_key] += line[1:].strip()
        elif "=" in line:
            k, _, v = line.partition("=")
            current_key = k.strip()
            info[current_key] = v.strip()

    state_dict: dict[str, int] = {}
    for kv in info.get("state_count", "").split():
        if ":" in kv:
            k, v = kv.split(":")
            try:
                state_dict[k] = int(v)
            except ValueError:
                pass

    def _to_int(s: str | None) -> int | None:
        try:
            return int(s) if s is not None else None
        except ValueError:
            return None

    return QueueStats(
        name=queue_name,
        total_jobs=int(info.get("total_jobs", 0) or 0),
        running=state_dict.get("Running", 0),
        queued=state_dict.get("Queued", 0),
        ncpus_assigned=_to_int(info.get("resources_assigned.ncpus")),
        ncpus_max=_to_int(info.get("resources_max.ncpus")),
    )
