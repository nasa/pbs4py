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
    """Return all job ids (active + finished history) for the user."""
    user = user or getpass.getuser()
    result = subprocess.run(
        ["qselect", "-x", "-u", user],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return [
        line.strip()
        for line in result.stdout.decode("utf-8", errors="replace").splitlines()
        if line.strip()
    ]


def get_user_jobs(user: str | None = None) -> list[PBSJob]:
    jobs = []
    for jid in _get_user_job_ids(user):
        try:
            jobs.append(PBSJob(jid))
        except Exception:
            print(f"\n=== Failed to load PBSJob for id: {jid} ===", flush=True)
            result = subprocess.run(
                ["qstat", "-xf", jid],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            print("--- stdout ---", flush=True)
            print(result.stdout.decode("utf-8", errors="replace"), flush=True)
            print("--- stderr ---", flush=True)
            print(result.stderr.decode("utf-8", errors="replace"), flush=True)
            print(f"--- returncode: {result.returncode} ---", flush=True)
            raise
    return jobs


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
    return active, finished[:n_recent_finished]
