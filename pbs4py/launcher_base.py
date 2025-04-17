import os
import subprocess
from typing import List
import numpy as np


class Launcher:
    def __init__(self, ncpus_per_node: int, ngpus_per_node: int, queue_node_limit: int, time: int,
                 profile_filename: str, requested_number_of_nodes: int):

        #: The hashbang line which sets the shell for the PBS script.
        #: If unset, the default is ``#!/usr/bin/env {self.shell}``.
        self.hashbang: str = None

        #: The shell flavor to use in the PBS job
        self.shell = 'bash'

        #: The maximum number nodes allowed by the queue
        self.queue_node_limit: int = queue_node_limit

        #: The number of compute nodes requested
        self.requested_number_of_nodes: int = requested_number_of_nodes

        #: The requested wall time for the pbs job(s) in hours
        self.time: int = time

        #: The number of CPU cores per node.
        self.ncpus_per_node: int = ncpus_per_node

        #: The number of GPUs per node.
        self.ngpus_per_node: int = ngpus_per_node

        #: The mpi execution command name: mpiexec, mpirun, mpiexec_mpt, etc.
        self.mpiexec: str = "mpiexec"
        self.mpiprocs_per_node = None

        #: Command line option for mpiexec to specify the number of MPI ranks for host/node.
        #: Default is to set it based on the mpiexec version.
        self.ranks_per_node_flag: str = None

        # these are properties that users typically don't need to set, but are
        # specific to each queueing software
        self.workdir_env_variable: str = ''
        self.profile_filename: str = ''
        self.batch_file_extension: str = ''

        #: If true, redirection of the output of mpi commands changed to tee
        self.tee_output: bool = False

        self.profile_filename = profile_filename

    @property
    def requested_number_of_nodes(self):
        """
        The number of nodes to request. That is, the 'select' number in the
        ``#PBS -l select={requested_number_of_nodes}:ncpus=40:mpiprocs=40``.

        :type: int
        """
        return self._requested_number_of_nodes

    @requested_number_of_nodes.setter
    def requested_number_of_nodes(self, number_of_nodes):
        self._requested_number_of_nodes = np.min((number_of_nodes, self.queue_node_limit))

    @property
    def mpiprocs_per_node(self):
        """
        The number of requested mpiprocs per node. If not set, the launcher will default
        to the number of cpus per node.
        ``#PBS -l select=1:ncpus=40:mpiprocs={mpiprocs_per_node}``.

        :type: int
        """
        if self._mpiprocs_per_node is None:
            return self.ncpus_per_node
        else:
            return self._mpiprocs_per_node

    @mpiprocs_per_node.setter
    def mpiprocs_per_node(self, mpiprocs):
        self._mpiprocs_per_node = mpiprocs

    @property
    def profile_filename(self):
        """
        The file to source at the start of the pbs script to set the environment.
        Typical names include '~/.profile', '~/.bashrc', and '~/.cshrc'.
        If you do not wish to source a file, set to ''.

        :type: str
        """
        return self._profile_filename

    @profile_filename.setter
    def profile_filename(self, profile_filename):
        if (profile_filename == '' or
                os.path.isfile(os.path.expanduser(profile_filename))):
            self._profile_filename = profile_filename
        else:
            raise FileNotFoundError('Unable to set profile file.')

    def create_mpi_command(
            self, command: str, output_root_name: str = None, openmp_threads: int = None,
            ranks_per_node: int = None) -> str:
        """
        Wrap a command with mpiexec and route its standard and error output to a file

        Parameters
        ----------
        command:
            The command thats needs to run in parallel
        output_root_name:
            The root name of the output file, {output_root_name}.out.
        openmp_threads:
            The number of openmp threads per mpi process.
        ranks_per_node:
            The number of MPI ranks per compute node.

        Returns
        -------
        full_command: str
            The full command string.
        """
        omp_env_vars = self._determine_omp_settings(openmp_threads)
        ranks_per_node_info = self._set_ranks_per_node_info(openmp_threads, ranks_per_node)
        openmp_info = self._set_openmp_info(openmp_threads)

        full_command = [omp_env_vars, self.mpiexec, ranks_per_node_info, openmp_info, command]
        if output_root_name is not None:
            redirect_output = self._redirect_shell_output(f"{output_root_name}.out")
            full_command.append(redirect_output)
        return self._filter_empty_strings_from_list_and_combine(full_command)

    def launch(self, job_name: str, job_body: List[str],
               blocking: bool = True, dependency: str = None) -> str:
        """
        Create a job script and launch the job

        Parameters
        ----------
        job_name:
            The name of the job.
        job_body:
            List of commands to run in the body of the job.
        blocking:
            If true, this function will wait for the job to complete before returning.
            If false, this function will launch the job but not wait for it to finish.
        dependency:
            Jobs that this one depends one. For PBS, these are colon separated in the string

        Returns
        -------
        command_output: str
            The stdout of the launch command. If the job is successfully launch,
            this will be the job id.
        """
        filename = f'{job_name}.{self.batch_file_extension}'
        self.write_job_file(filename, job_name, job_body, dependency)
        return self._run_job(filename, blocking)

    def write_job_file(self, job_filename: str, job_name: str,
                       job_body: List[str], dependency: str = None):
        """
        Create a launch script file in the current directory for the commands defined in ``job_body``.

        Parameters
        ----------
        job_filename:
            name of file to write to
        job_name:
            The name of the job.
        job_body:
            List of commands to run in the body of the job.
        dependency:
            Jobs that this one depends one. For PBS, these are colon separated in the string
        """
        with open(job_filename, mode='w') as fh:
            header = self._create_header(job_name, dependency)
            for line in header:
                fh.write(line + '\n')

            for _ in range(2):
                fh.write('\n')

            fh.write(f'cd {self.workdir_env_variable}\n')
            if len(self.profile_filename) > 0:
                fh.write(f'source {self.profile_filename}\n')

            for _ in range(1):
                fh.write('\n')

            for line in job_body:
                fh.write(line + '\n')

    def _create_header(self, job_name: str, dependency: str = None) -> List[str]:
        header = self._create_list_of_standard_header_options(job_name)
        header.extend(self._create_list_of_optional_header_lines(dependency))
        return header

    def _create_hashbang(self):
        if self.hashbang is not None:
            return self.hashbang
        else:
            return f'#!/usr/bin/env {self.shell}'

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        return ['']

    def _create_list_of_optional_header_lines(self, dependency: str) -> List[str]:
        return ['']

    def _run_job(self, job_filename: str, blocking: bool, print_command_output: bool = True) -> str:
        raise NotImplementedError('Launcher must implement a _run_job method')

    def _redirect_shell_output(self, output_filename):
        if self.tee_output:
            return f'2>&1 | tee {output_filename}'

        if self.shell == 'tcsh':
            return f'>& {output_filename}'
        else:
            return f'&> {output_filename}'

    def _use_omplace_command(self) -> bool:
        return self._using_mpt()

    def _use_openmp(self, openmp_threads: int = None):
        if openmp_threads is not None:
            if openmp_threads > 1:
                return True
        return False

    def _using_mpt(self) -> bool:
        if self.mpiexec == "mpiexec_mpt":
            return True

        try:
            output = subprocess.run(
                [self.mpiexec, "--version"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return "MPT" in output.stderr or "MPT" in output.stdout
        except FileNotFoundError:
            print(f"Executable '{self.mpiexec}' not found")
            return False

    def _get_ranks_per_node_flag(self):
        if self.ranks_per_node_flag is not None:
            return self.ranks_per_node_flag
        else:
            if self._using_mpt():
                return "-perhost"
            else:
                return "--npernode"

    def _determine_omp_settings(self, openmp_threads: int) -> str:
        if openmp_threads is None:
            return ""

        omp_env_vars = [f"OMP_NUM_THREADS={openmp_threads}"]
        if not self._use_omplace_command():
            omp_env_vars.extend(["OMP_PLACES=cores", "OMP_PROC_BIND=close"])
        return self._filter_empty_strings_from_list_and_combine(omp_env_vars)

    def _filter_empty_strings_from_list_and_combine(self, lis: List[str]) -> str:
        filtered_for_empty_strings = filter(None, lis)
        return " ".join(filtered_for_empty_strings)

    def _set_ranks_per_node_info(self, openmp_threads: int, ranks_per_node: int) -> str:
        if ranks_per_node is None and openmp_threads is None:
            return ""
        elif ranks_per_node is not None:
            mpi_procs_per_node = ranks_per_node
        else:  # openmp_threads is not None:
            mpi_procs_per_node = self.ncpus_per_node // openmp_threads

        ranks_per_node_flag = self._get_ranks_per_node_flag()
        ranks_per_proc_info = f"{ranks_per_node_flag} {mpi_procs_per_node}"
        return ranks_per_proc_info

    def _set_openmp_info(self, openmp_threads: int) -> str:
        if not self._use_openmp(openmp_threads):
            return ""

        openmp_info = ""
        if self._use_omplace_command():
            proc_num_list = ",".join([str(i) for i in range(self.ncpus_per_node)])
            openmp_info = f'omplace -c "{proc_num_list}" -nt {openmp_threads} -vv'
        return openmp_info
