#!/usr/bin/env python
import os
from typing import List, Union
import numpy as np

from pbs4py.launcher_base import Launcher


class PBS(Launcher):
    def __init__(self, queue_name: str = 'K4-route',
                 ncpus_per_node: int = 40,
                 queue_node_limit: int = 10,
                 time: int = 72,
                 profile_file: str = '~/.bashrc'):
        """
        | A class for creating and running pbs jobs. Default queue properties are for K4.
        | Defaults not set during instantiation can be adjusted by directly modifying attributes.

        Parameters
        ----------
        queue_name:
            Queue name which goes on the "#PBS -N {name}" line of the pbs header
        ncpus_per_node:
            Number of CPU cores per node
        queue_node_limit:
            Maximum number of nodes allowed in this queue
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job. Set to
            '' if you do not wish to source a file.
        """
        #: The maximum number nodes allowed by the queue
        self.queue_node_limit: int = queue_node_limit

        super().__init__()

        #: The name of the queue which goes on the ``#PBS -N {queue_name}``
        #: line of the pbs header
        self.queue_name: str = queue_name

        #: The number of CPU cores per node.
        self.ncpus_per_node: int = ncpus_per_node

        #: The requested wall time for the pbs job(s) in hours
        self.time: int = time

        #: The processor model if it needs to be specified.
        #: The associated PBS header line is ``#PBS -l select=#:ncpus=#:mpiprocs=#:model={model}``
        #: If left as `None`, the ``:model={mode}`` will not be added to the header line
        self.model: Union[str, None] = None

        #: The group for the group_list entry of the pbs header if necessary.
        #: The associated PBS header line is ``#PBS -W group_list={group_list}``
        self.group_list: Union[str, None] = None

        #: Requested memory size on the select line. Need to include units in the str.
        #: The associated PBS header line is ``#PBS -l select=#:mem={mem}``
        self.mem: Union[str, None] = None

        #: Index range for PBS array of jobs
        #: The associated PBS header line is ``#PBS -J {array_range}``
        self.array_range: Union[str, None] = None

        #: The mpi execution command name: mpiexec, mpirun, mpiexec_mpt, etc.
        self.mpiexec: str = 'mpiexec'

        #: ``pbs -m`` mail options. 'e' at exit, 'b' at beginning, 'a' at abort
        self.mail_options: str = None

        #: ``pbs -M`` mail list. Who to email when mail_options are triggered
        self.mail_list: Union[str, None] = None

        #: Type of dependency if dependency active.
        #: Default is 'afterok' which only launches the new job if the previous one was successful.
        self.dependency_type: str = 'afterok'

        self.profile_filename = profile_file
        self.workdir_env_variable = '$PBS_O_WORKDIR'
        self.batch_file_extension = 'pbs'
        self.mpiprocs_per_node = None

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

    def create_mpi_command(self, command: str,
                           output_root_name: str,
                           openmp_threads: int = None) -> str:
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

        Returns
        -------
        full_command: str
            The full command string.
        """
        omp_env_vars = self._determine_omp_settings(openmp_threads)
        proc_info = self._set_proc_and_omplace_info(openmp_threads)

        redirect_output = self._redirect_shell_output(f'{output_root_name}.out')
        full_command = [omp_env_vars, self.mpiexec, proc_info, command, redirect_output]
        return self._filter_empty_strings_from_list_and_combine(full_command)

    def _use_omplace_command(self) -> bool:
        return self.mpiexec == 'mpiexec_mpt'

    def _use_openmp(self, openmp_threads: int or None):
        if type(openmp_threads) == int:
            if openmp_threads > 1:
                return True
        return False

    def _determine_omp_settings(self, openmp_threads: int) -> str:
        if openmp_threads is None:
            return ''

        omp_env_vars = [f'OMP_NUM_THREADS={openmp_threads}']
        if not self._use_omplace_command():
            omp_env_vars.extend(['OMP_PLACES=cores', 'OMP_PROC_BIND=close'])
        return self._filter_empty_strings_from_list_and_combine(omp_env_vars)

    def _filter_empty_strings_from_list_and_combine(self, lis: List[str]) -> str:
        filtered_for_empty_strings = filter(None, lis)
        return ' '.join(filtered_for_empty_strings)

    def _set_proc_and_omplace_info(self, openmp_threads: int) -> str:
        if not self._use_openmp(openmp_threads):
            return ''

        total_num_mpi_procs = (self.ncpus_per_node //
                               openmp_threads) * self.requested_number_of_nodes

        proc_info = f'-np {total_num_mpi_procs}'
        if self._use_omplace_command():
            proc_num_list = ','.join([str(i) for i in range(self.ncpus_per_node)])
            proc_info += f' omplace -c "{proc_num_list}" -nt {openmp_threads} -vv'
        return proc_info

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        header_lines = [self._create_hashbang(),
                        self._create_job_line_of_header(job_name),
                        self._create_queue_line_of_header(),
                        self._create_select_line_of_header(),
                        self._create_walltime_line_of_header(),
                        self._create_log_name_line_of_header(job_name),
                        self._create_header_line_to_join_standard_and_error_output(),
                        self._create_header_line_to_set_that_job_is_not_rerunnable()]
        return header_lines

    def _create_job_line_of_header(self, job_name: str) -> str:
        return f'#PBS -N {job_name}'

    def _create_queue_line_of_header(self) -> str:
        return f'#PBS -q {self.queue_name}'

    def _create_select_line_of_header(self) -> str:
        select = f'select={self.requested_number_of_nodes}'
        ncpus = f'ncpus={self.ncpus_per_node}'
        mpiprocs = f'mpiprocs={self.mpiprocs_per_node}'

        select_line = f'#PBS -l {select}:{ncpus}:{mpiprocs}'
        if self.mem is not None:
            select_line += f':mem={self.mem}'
        if self.model is not None:
            select_line += f':model={self.model}'
        return select_line

    def _create_walltime_line_of_header(self) -> str:
        return f'#PBS -l walltime={self.time}:00:00'

    def _create_log_name_line_of_header(self, job_name: str) -> str:
        return f'#PBS -o {job_name}_pbs.log'

    def _create_header_line_to_join_standard_and_error_output(self):
        return '#PBS -j oe'

    def _create_header_line_to_set_that_job_is_not_rerunnable(self) -> str:
        return '#PBS -r n'

    def _create_list_of_optional_header_lines(self, dependency):
        header_lines = []
        header_lines.extend(self._create_group_list_header_line())
        header_lines.extend(self._create_array_range_header_line())
        header_lines.extend(self._create_mail_options_header_lines())
        header_lines.extend(self._create_job_dependencies_header_line(dependency))
        return header_lines

    def _create_group_list_header_line(self) -> List[str]:
        if self.group_list is not None:
            return [f'#PBS -W group_list={self.group_list}']
        else:
            return []

    def _create_array_range_header_line(self) -> List[str]:
        if self.array_range is not None:
            return [f'#PBS -J {self.array_range}']
        else:
            return []

    def _create_mail_options_header_lines(self) -> List[str]:
        header_lines = []
        if self.mail_options is not None:
            header_lines.append(f'#PBS -m {self.mail_options}')
        if self.mail_list is not None:
            header_lines.append(f'#PBS -M {self.mail_list}')
        return header_lines

    def _create_job_dependencies_header_line(self, dependency) -> List[str]:
        if dependency is not None:
            return [f'#PBS -W depend={self.dependency_type}:{dependency}']
        else:
            return []

    def _run_job(self, job_filename: str, blocking: bool, print_command_output=True) -> str:
        options = ''
        if blocking:
            options += '-Wblock=true'
        command_output = os.popen(f'qsub {options} {job_filename}').read().strip()
        if print_command_output:
            print(command_output)
        return command_output

    # Alternate constructors for NASA HPC queues
    @classmethod
    def k4(cls, time: int = 72, profile_file: str = '~/.bashrc'):
        """
        Constructor for the K4 queues on LaRC's K cluster including K4-standard-512.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        """
        return cls(queue_name='K4-route', ncpus_per_node=40,
                   queue_node_limit=16, time=time,
                   profile_file=profile_file)

    @classmethod
    def k3(cls, time: int = 72, profile_file: str = '~/.bashrc'):
        """
        Constructor for the K3 queues on LaRC's K cluster including K3-standard-512.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        """
        return cls(queue_name='K3-route', ncpus_per_node=16,
                   queue_node_limit=40, time=time,
                   profile_file=profile_file)

    @classmethod
    def k3a(cls, time: int = 72, profile_file: str = '~/.bashrc'):
        """
        Constructor for the K3a queue on LaRC's K cluster.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        """
        return cls(queue_name='K3a-route', ncpus_per_node=16,
                   queue_node_limit=25, time=time,
                   profile_file=profile_file)

    @classmethod
    def nas(cls, group_list: str, proc_type: str = 'broadwell', queue_name: str = 'long',
            time: int = 72, profile_file: str = '~/.bashrc'):
        """
        Constructor for the queues at NAS. Must specify the group_list

        Parameters
        ----------
        group_list:
            The charge number or group for the group_list entry of the pbs header.
            The associated PBS header line is "#PBS -W group_list={group_list}".
        proc_type:
            The type of processor to submit to. Can write out or just the first 3 letters:
            'cas', 'sky', 'bro', 'has', 'ivy', 'san'.
        queue_name:
            Which queue to submit to: devel, debug, normal, long, etc.
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        """

        if 'cas' in proc_type.lower():
            ncpus_per_node = 40
            model = 'cas_ait'
        elif 'sky' in proc_type.lower():
            ncpus_per_node = 40
            model = 'sky_ele'
        elif 'bro' in proc_type.lower():
            ncpus_per_node = 28
            model = 'bro'
        elif 'has' in proc_type.lower():
            ncpus_per_node = 24
            model = 'has'
        elif 'ivy' in proc_type.lower():
            ncpus_per_node = 20
            model = 'ivy'
        elif 'san' in proc_type.lower():
            ncpus_per_node = 20
            model = 'san'
        else:
            raise ValueError('Unknown NAS processor selection')

        pbs = cls(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
                  queue_node_limit=int(1e6), time=time, profile_file=profile_file)

        pbs.group_list = group_list
        pbs.model = model
        return pbs


class FakePBS(PBS):
    def __init__(self, profile_file=''):
        """
        A fake PBS class for directly running commands while still calling as
        if it were a standard PBS driver.
        This can be used to seemless switch between modes where PBS jobs are
        launched for each "job", or using a FakePBS object when you don't want to
        launch a new pbs job for each "job", e.g., driving a script
        while already within the PBS job.
        """
        super().__init__(profile_file=profile_file)

    def launch(self, job_name: str, job_body: List[str],
               blocking: bool = True, dependency: str = None) -> str:
        """
        Runs the commands in the job_body

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
        for line in job_body:
            print(line)
            os.system(line)
        return ''
