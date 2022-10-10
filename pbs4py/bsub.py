import os
from typing import List

from pbs4py.launcher_base import Launcher


class BSUB(Launcher):
    def __init__(self,
                 project: str,
                 ngpu_per_node: int = 6,
                 time: int = 72,
                 profile_filename: str = '~/.bashrc'):
        """
        A Class for creating and running jobs using the Department of Energy
        batch system.

        Parameters
        ----------
        project:
            The project which to charge for submitted jobs
        ngpu_per_node:
            The number of GPUs per compute node
        time:
            The requested wall time for the job(s) in hours
        profile_filename:
            The file setting the environment to source inside the PBS job
        """
        super().__init__()

        #: The requested wall time for the job(s) in hours
        self.time: int = time

        #: The number of GPUs per compute node.
        self.ngpu_per_node: int = ngpu_per_node

        #: The project which to charge for submitted jobs
        self.project: str = project

        #: Mail a job report when complete
        self.mail_when_complete: bool = True

        self.profile_filename = profile_filename
        self.workdir_env_variable = '$LS_SUBCWD'
        self.batch_file_extension = 'lsf'

    def create_mpi_command(self, command: str,
                           output_root_name: str,
                           openmp_threads: int = 1) -> str:
        num_mpi_procs = self.requested_number_of_nodes * self.ngpu_per_node
        redirect_output = self._redirect_shell_output(f'{output_root_name}.out')
        command = f'jsrun -n {num_mpi_procs} -a 1 -c {openmp_threads} -g 1 {command} {redirect_output}'
        return command

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        header_lines = [self._create_hashbang(),
                        self._create_project_line_of_header(),
                        self._create_job_name_line_of_header(job_name),
                        self._create_number_of_nodes_line_of_header(),
                        self._create_wall_time_line_of_header()]
        return header_lines

    def _create_project_line_of_header(self) -> str:
        return f'#BSUB -P {self.project}'

    def _create_job_name_line_of_header(self, job_name: str) -> str:
        return f'#BSUB -J {job_name}'

    def _create_number_of_nodes_line_of_header(self) -> str:
        return f'#BSUB -nnodes {self.requested_number_of_nodes}'

    def _create_wall_time_line_of_header(self) -> str:
        return f'#BSUB -W {self.time}:00'

    def _create_list_of_optional_header_lines(self, dependency: str) -> List[str]:
        header_lines = []
        header_lines.extend(self._create_job_dependency_header_line(dependency))
        header_lines.extend(self._create_mail_header_line())
        return header_lines

    def _create_job_dependency_header_line(self, dependency: str) -> List[str]:
        if dependency is not None:
            return [f'#BSUB -w ended({dependency})']
        else:
            return []

    def _create_mail_header_line(self) -> List[str]:
        if self.mail_when_complete:
            return [f'#BSUB -N']
        else:
            return []

    def _run_job(self, job_filename: str, blocking: bool, print_command_output: bool = True) -> str:
        if blocking:
            print('Warning: Blocking for bsub not implemented')

        command = f'bsub {job_filename}'
        if print_command_output:
            print(command)
        return os.popen(command).read()

    def _parse_job_id_out_of_bsub_output(self, bsub_output: str) -> int:
        return int(bsub_output.split('>')[0].split('<')[-1])
