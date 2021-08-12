import os
import numpy as np
import time
from datetime import datetime
from typing import List
from pyrefine.pbs import PBS
from pyrefine.directory_utils import cd

class Job:
    def __init__(self, name: str, body: List[str]):
        """
        Class for individual PBS jobs within a batch of jobs

        Can be used as a context manager to enter/exit a directory
        with the job's name
        """
        self.name = name
        self.body = body
        self.id = None

    def get_pbs_job_state(self):
        if self.id is not None:
            return os.popen(f'qstat -xf {self.id} | grep job_state').read().split()[-1]
        else:
            return ''

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.name)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

class PBSBatch:
    def __init__(self, pbs: PBS, jobs: List[Job], use_separate_directories = True):
        """
        Nonblocking batch of PBS jobs. Assumes all jobs required the same
        job request size. By default, separate directories with the job's name
        will be created and each pbs job will be launched in there to
        separate output files.

        Parameters
        ----------
        pbs:
            PBS handler that will be used to submit the jobs
        jobs:
            List of Job objects that will be run
        use_separate_directories:
            whether to run each job in a separate directory with the job's name
        """
        self.pbs = pbs
        self.jobs = jobs
        self.use_separate_directories = use_separate_directories

    def create_directories(self):
        """
        Create the set of directories with the jobs' names
        """
        for job in self.jobs:
            if not os.path.exists(job.name):
                os.mkdir(job.name)

    def launch_all_jobs(self, wait_for_jobs_to_finish = False,
                              check_frequency_in_secs = 30):
        """
        Launch of the all of the jobs in the list. Stores the pbs
        job id in the job objects
        """
        self._launch_jobs_in_a_list(self.jobs)
        if wait_for_jobs_to_finish:
            self.wait_for_all_jobs_to_finish(check_frequency_in_secs=check_frequency_in_secs)

    def launch_jobs_with_limit(self, max_jobs_at_a_time = 20, check_frequency_in_secs = 30):
        """
        The "courteous" version of launch_all_jobs(wait_for_jobs_to_finish=True) and where only
        a maximum number of jobs will be running or in the queue at a time
        """
        total_num_of_jobs = len(self.jobs)

        next_job_to_submit = 0
        while True:
            states = self._get_job_states(self.jobs[:next_job_to_submit])
            num_active_jobs = self._count_number_of_jobs_running_queued_or_held(states)
            if num_active_jobs < max_jobs_at_a_time:
                end_index = np.min((total_num_of_jobs, next_job_to_submit + max_jobs_at_a_time - num_active_jobs))
                self._launch_jobs_in_a_list(self.jobs[next_job_to_submit:end_index])
                next_job_to_submit = end_index

            states = self._get_job_states(self.jobs[:next_job_to_submit])
            self._print_summary_of_job_states(states)
            if self._all_jobs_submitted(next_job_to_submit):
                if not self._any_jobs_are_still_running_queued_or_held(states):
                    break
            time.sleep(check_frequency_in_secs)

    def _launch_jobs_in_a_list(self, jobs: List[Job]):
        for job in jobs:
            dirname = job.name if self.use_separate_directories else '.'
            with cd(dirname):
                job.id = self.pbs.launch(job.name, job.body, blocking=False)

    def wait_for_all_jobs_to_finish(self, check_frequency_in_secs = 30):
        """
        A blocking check for all the jobs in the batch to finish

        Parameters
        ----------
        check_frequency_in_secs:
            How often to check and print the jobs' states
        """
        while True:
            states = self._get_job_states(self.jobs)
            self._print_summary_of_job_states(states)
            if self._any_jobs_are_still_running_queued_or_held(states):
                time.sleep(check_frequency_in_secs)
            else:
                break

    def _all_jobs_submitted(self, next_job_to_submit):
        return next_job_to_submit == len(self.jobs)

    def _get_job_states(self, jobs: List[Job]) -> List[str]:
        states = []
        for job in jobs:
            states.append(job.get_pbs_job_state())
        return states

    def _count_number_of_jobs_running_queued_or_held(self, pbs_states: List[str]):
        return pbs_states.count('R') + pbs_states.count('Q') + pbs_states.count('H')

    def _any_jobs_are_still_running_queued_or_held(self, pbs_states):
        return self._count_number_of_jobs_running_queued_or_held(pbs_states) > 0

    def _print_summary_of_job_states(self, states: List[str]):
        running = states.count('R')
        queued = states.count('Q')
        finished = states.count('F')
        other = len(states) - running - queued - finished
        print(f'Job states at {datetime.now().isoformat()}:')
        print(f'  Queued:        {queued}')
        print(f'  Running:       {running}')
        print(f'  Finished:      {finished}')

        num_of_jobs_not_yet_submitted = len(self.jobs)-len(states)
        if num_of_jobs_not_yet_submitted > 0:
            print(f'  Yet to submit: {num_of_jobs_not_yet_submitted}')
        print(f'  Other:         {other}')
