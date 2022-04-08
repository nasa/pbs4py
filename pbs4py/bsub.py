from pbs4py.batch_launcher import BatchLauncher
from typing import List


class BSUB(BatchLauncher):
    def __init__(self,
                 time: int = 72,
                 profile_file: str = '~/.bashrc'):
        self.time = time
        self.profile_file = profile_file
        self.requested_number_of_nodes = 1

        self.workdir_env_variable = '$LS_SUBCWD'
        self.batch_file_extension = 'lsf'

        self.requested_number_of_nodes = 1
        self.number_of_gpus_per_node = 6

    def create_mpi_command(self, command: str,
                           output_root_name: str,
                           openmp_threads: int = None) -> str:
        num_mpi_procs = self.requested_number_of_nodes * self.number_of_gpus_per_node
        command = f'jsrun -n {num_mpi_procs} -a 1 -c 1 -g 1 {command} > {output_root_name}.out >2>&1'
        return command

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        return

    def _create_list_of_optional_header_lines(self, dependency: str) -> List[str]:
        return ['']

    def _run_job(self, job_filename: str, blocking: bool, print_command_output: bool = True) -> str:
        return ''
