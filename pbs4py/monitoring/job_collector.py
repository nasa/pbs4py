"""
Collect the current user's PBS jobs as PBSJob instances.
"""

from __future__ import annotations

import getpass
import os
import subprocess
from datetime import datetime

from pbs4py.job import PBSJob


def _get_user_job_ids(user: str | None = None) -> list[str]:
    """Return all job ids (active + recently finished) for the user."""
    user = user or getpass.getuser()
    result = subprocess.run(
        ["qstat", "-xu", user],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    job_ids: list[str] = []
    for line in result.stdout.decode("utf-8", errors="replace").splitlines():
        # qstat -xu lines starting with a digit are job rows; the first token is the id.
        token = line.split(None, 1)[0] if line.strip() else ""
        if token and token[0].isdigit():
            job_ids.append(token)
    return job_ids


def get_user_jobs(user: str | None = None) -> list[PBSJob]:
    return [PBSJob(jid) for jid in _get_user_job_ids(user)]


def has_dog_out(job: PBSJob) -> bool:
    return bool(job.workdir) and os.path.isfile(os.path.join(job.workdir, "dog.out"))


def dog_out_path(job: PBSJob) -> str:
    return os.path.join(job.workdir, "dog.out") if job.workdir else ""


def split_active_and_finished(
    jobs: list[PBSJob], n_recent_finished: int = 10
) -> tuple[list[PBSJob], list[PBSJob]]:
    active = [j for j in jobs if j.state in ("Q", "R", "H", "E", "B", "W", "T")]
    finished = [j for j in jobs if j.state == "F"]
    finished.sort(key=lambda j: j.mtime or datetime.min, reverse=True)
    return active, finished
