import os
import time
from datetime import datetime

from .pbs import PBS
from .directory_utils import cd


class BatchJob:
    def __init__(self, name: str, body: list[str]):
        """
        Class for individual PBS jobs within a batch of jobs.

        Can be used as a context manager to enter/exit a directory
        with the job's name.
        """
        #: Name of the job.
        self.name = name

        #: List of commands to run in PBS job.
        self.body = body

        #: PBS job identifier returned by qsub.
        self.id: str | None = None

        #: Saved working directory used by the context manager.
        self._saved_path: str | None = None

    def get_pbs_job_state(self) -> str:
        """
        Get the job's status after it has been submitted.

        Returns the entry of ``job_state`` in the qstat information
        (e.g., ``'Q'``, ``'R'``, ``'F'``, ``'H'``). Returns an empty string
        if the job has not been submitted yet.
        """
        if self.id is None:
            return ""
        return os.popen(f"qstat -xf {self.id} | grep job_state").read().split()[-1]

    def __enter__(self):
        self._saved_path = os.getcwd()
        os.chdir(self.name)
        return self

    def __exit__(self, etype, value, traceback):
        os.chdir(self._saved_path)


class PBSBatch:
    def __init__(
        self,
        pbs: PBS,
        jobs: list[BatchJob],
        use_separate_directories: bool = True,
    ):
        """
        Batch of PBS jobs. Assumes all jobs require the same job request size.
        By default, separate directories with each job's name will be used to
        separate output files.

        Parameters
        ----------
        pbs:
            PBS handler that will be used to submit the jobs.
        jobs:
            List of :class:`BatchJob` objects that will be run.
        use_separate_directories:
            Whether to run each job in a separate directory with the job's name.
        """
        self.pbs = pbs
        self.jobs = jobs
        self.use_separate_directories = use_separate_directories

    def create_directories(self) -> None:
        """Create the set of directories with the jobs' names."""
        for job in self.jobs:
            os.makedirs(job.name, exist_ok=True)

    def launch_all_jobs(
        self,
        wait_for_jobs_to_finish: bool = False,
        check_frequency_in_secs: float = 30,
    ) -> None:
        """
        Launch all of the jobs in the list. Stores the PBS job id in the
        job objects.

        Parameters
        ----------
        wait_for_jobs_to_finish:
            If True, the jobs will be submitted and this function will not
            return until all of the jobs are finished.
        check_frequency_in_secs:
            Time interval to wait before checking if all jobs are done. Only
            relevant if ``wait_for_jobs_to_finish`` is True.
        """
        self._launch_jobs_in_a_list(self.jobs)
        if wait_for_jobs_to_finish:
            self.wait_for_all_jobs_to_finish(check_frequency_in_secs=check_frequency_in_secs)

    def launch_jobs_with_limit(
        self,
        max_jobs_at_a_time: int = 20,
        check_frequency_in_secs: float = 30,
    ) -> None:
        """
        The "courteous" version of ``launch_all_jobs(wait_for_jobs_to_finish=True)``
        where a limit is set for the maximum number of jobs running or queued at a
        time, since some sites may not appreciate submitting 1000 jobs at once.

        Parameters
        ----------
        max_jobs_at_a_time:
            Limit for number of jobs to have queued, running, or held at a time.
        check_frequency_in_secs:
            Time interval to wait before checking jobs' statuses.
        """
        total_num_of_jobs = len(self.jobs)
        next_job_to_submit = 0

        while True:
            states = self._get_job_states(self.jobs[:next_job_to_submit])
            num_active_jobs = self._count_number_of_jobs_running_queued_or_held(states)

            if num_active_jobs < max_jobs_at_a_time:
                end_index = min(
                    total_num_of_jobs,
                    next_job_to_submit + max_jobs_at_a_time - num_active_jobs,
                )
                self._launch_jobs_in_a_list(self.jobs[next_job_to_submit:end_index])
                next_job_to_submit = end_index

            states = self._get_job_states(self.jobs[:next_job_to_submit])
            self._print_summary_of_job_states(states)

            if self._all_jobs_submitted(next_job_to_submit):
                if not self._any_jobs_are_still_running_queued_or_held(states):
                    break
            time.sleep(check_frequency_in_secs)

    def wait_for_all_jobs_to_finish(self, check_frequency_in_secs: float = 30) -> None:
        """
        Blocking check for all the jobs in the batch to finish. Can be paired
        with :meth:`launch_all_jobs`.

        Parameters
        ----------
        check_frequency_in_secs:
            How often to check and print the jobs' states.
        """
        while True:
            states = self._get_job_states(self.jobs)
            self._print_summary_of_job_states(states)
            if not self._any_jobs_are_still_running_queued_or_held(states):
                break
            time.sleep(check_frequency_in_secs)

    def _launch_jobs_in_a_list(self, jobs: list[BatchJob]) -> None:
        for job in jobs:
            dirname = job.name if self.use_separate_directories else "."
            with cd(dirname):
                job.id = self.pbs.launch(job.name, job.body, blocking=False)

    def _all_jobs_submitted(self, next_job_to_submit: int) -> bool:
        return next_job_to_submit == len(self.jobs)

    def _get_job_states(self, jobs: list[BatchJob]) -> list[str]:
        return [job.get_pbs_job_state() for job in jobs]

    def _count_number_of_jobs_running_queued_or_held(self, pbs_states: list[str]) -> int:
        return sum(pbs_states.count(s) for s in ("R", "Q", "H"))

    def _any_jobs_are_still_running_queued_or_held(self, pbs_states: list[str]) -> bool:
        return self._count_number_of_jobs_running_queued_or_held(pbs_states) > 0

    def _print_summary_of_job_states(self, states: list[str]) -> None:
        running = states.count("R")
        queued = states.count("Q")
        finished = states.count("F")
        other = len(states) - running - queued - finished

        print(f"Job states at {datetime.now().isoformat()}:")
        print(f"  Queued:        {queued}")
        print(f"  Running:       {running}")
        print(f"  Finished:      {finished}")

        num_of_jobs_not_yet_submitted = len(self.jobs) - len(states)
        if num_of_jobs_not_yet_submitted > 0:
            print(f"  Yet to submit: {num_of_jobs_not_yet_submitted}")
        print(f"  Other:         {other}")
