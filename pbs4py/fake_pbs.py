from typing import List
import subprocess
from pbs4py import PBS


class FakePBS(PBS):
    """
    A fake PBS class for directly running commands while still calling as
    if it were a standard PBS driver.
    This can be used to seemless switch between modes where PBS jobs are
    launched for each "job", or using a FakePBS object when you don't want to
    launch a new pbs job for each "job", e.g., driving a script
    while already within the PBS job.
    """

    def __init__(self, profile_file='', stop_at_first_failure=False):
        super().__init__(profile_file=profile_file)
        self.stop_at_first_failure = stop_at_first_failure

    def launch(self, job_name: str, job_body: List[str],
               blocking: bool = True, dependency: str = None) -> str:
        """
        Runs the commands in the job_body and determines if any failed
        based on status flags

        Parameters
        ----------
        job_name:
            [ignored]
        job_body:
            List of commands to run
        blocking:
            [ignored]
        dependency:
            [ignored]

        Returns
        -------
        pbs_command_output: str
            Empty string but returning something to match true PBS launch output
        """

        number_of_failures = 0
        for line in job_body:
            print(line)
            process = subprocess.Popen(line, shell=True)
            process.wait()

            if process.returncode != 0:
                number_of_failures += 1
                if self.stop_at_first_failure:
                    break
        return f'FakePBS.{number_of_failures}'
