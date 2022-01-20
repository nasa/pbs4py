#!/usr/bin/env python
import os
from typing import List, Union
import numpy as np


class PBS:
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
            The file setting the environment to source inside the PBS job
        """

        #: The name of the queue which goes on the ``#PBS -N {queue_name}``
        #: line of the pbs header
        self.queue_name: str = queue_name

        #: The number of CPU cores per node.
        self.ncpus_per_node: int = ncpus_per_node

        #: The maximum number nodes allowed by the queue
        self.queue_node_limit: int = queue_node_limit

        #: The requested wall time for the pbs job(s) in hours
        self.time: int = time

        #: The number of mpi ranks per node. The default behavior is to set
        #: ``mpiprocs_per_node = ncpus_per_node`` where ncpus_per_node is
        #: the value given during instantiation. For standard MPI execution,
        #: For mpi+openMP, ``mpiprocs_per_node`` may be less than ``ncpus_per_node``.
        self.mpiprocs_per_node: int = ncpus_per_node

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

        #: The hashbang line which sets the shell for the PBS script.
        #: If unset, the default is ``#!/usr/bin/env bash``.
        self.hashbang: str = '#!/usr/bin/env bash'

        #: The mpi execution command name: mpiexec, mpirun, mpiexec_mpt, etc.
        self.mpiexec: str = 'mpiexec'

        #: ``pbs -m`` mail options. 'e' at exit, 'b' at beginning, 'a' at abort
        self.mail_options: str = None

        #: ``pbs -M`` mail list. Who to email when mail_options are triggered
        self.mail_list: Union[str, None] = None

        #: Type of dependency if dependency active.
        #: Default is 'afterok' which only launches the new job if the previous one was successful.
        self.dependency_type: str = 'afterok'

        self.profile_filename: str = profile_file
        self.requested_number_of_nodes: int = 1

    @property
    def profile_filename(self):
        """
        The file to source at the start of the pbs script to set the environment.
        Typical names include '~/.profile', '~/.bashrc', and '~/.cshrc'.
        The default profile filename is '~/.bashrc'

        :type: str
        """
        return self._profile_filename

    @profile_filename.setter
    def profile_filename(self, profile_filename):
        if os.path.exists(os.path.expanduser(profile_filename)):
            self._profile_filename = profile_filename
        else:
            raise FileNotFoundError('Unable to set profile file.')

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
        proc_info = ' '
        omp_env_variable = ''
        if openmp_threads is not None:
            omp_env_variable += f'OMP_NUM_THREADS={openmp_threads} '

            num_mpi_procs = (self.ncpus_per_node // openmp_threads) * self.requested_number_of_nodes
            proc_info += f'-np {num_mpi_procs} '
            self.mpiprocs_per_node = (self.ncpus_per_node // openmp_threads)

            if self.mpiexec == 'mpiexec_mpt':
                procs = ','.join([str(i) for i in range(self.ncpus_per_node)])
                proc_info += f'omplace -c "{procs}" -nt {openmp_threads} -vv '
            else:
                omp_env_variable += 'OMP_PLACES=cores OMP_PROC_BIND=close '
        else:
            self.mpiprocs_per_node = self.ncpus_per_node

        direct_output = f' > {output_root_name}.out 2>&1'
        full_command = omp_env_variable + self.mpiexec + proc_info + command + direct_output
        return full_command

    def launch(self, job_name: str, job_body: List[str],
               blocking: bool = True, dependency: str = None) -> str:
        """
        Create a pbs script and launch the job

        Parameters
        ----------
        job_name:
            The name of the job.
        job_body:
            List of commands to run in the body of the PBS job.
        blocking:
            If true, this function will wait for the PBS job to complete before returning.
            If false, this function will launch the job but not wait for it to finish.
        dependency:
            PBS jobs or colon separated jobs in a string that this one depends one.

        Returns
        -------
        pbs_command_output: str
            The stdout of the pbs command. If the pbs job is successfully launch,
            this will be the pbs job id.
        """
        pbs_file = f'{job_name}.pbs'
        self.write_pbs_file(pbs_file, job_name, job_body, dependency)
        return self._run_pbs_job(pbs_file, blocking)

    def write_pbs_file(self, pbs_filename: str, job_name: str,
                       job_body: List[str], dependency: str = None):
        """
        Create a pbs script file in the current directory using the pbs properties and commands defined in ``job_body``.

        Parameters
        ----------
        pbs_file:
            name of file to write to
        job_name:
            The name of the job.
        job_body:
            List of commands to run in the body of the PBS job.
        dependency:
            PBS jobs or colon separated jobs that this one depends one.
        """
        with open(pbs_filename, mode='w') as fh:
            pbs_header = self._create_pbs_header(job_name, dependency)
            for line in pbs_header:
                fh.write(line + '\n')

            for _ in range(2):
                fh.write('\n')

            fh.write('cd $PBS_O_WORKDIR\n')
            fh.write('source %s\n' % self.profile_filename)

            for _ in range(1):
                fh.write('\n')

            for line in job_body:
                fh.write(line + '\n')

    def _create_pbs_header(self, job_name: str, dependency: str = None) -> List[str]:
        pbs_header = self._create_list_of_standard_header_options(job_name)
        pbs_header.extend(self._create_list_of_optional_header_lines(dependency))
        return pbs_header

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        header_lines = [self.hashbang,
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

    def _run_pbs_job(self, pbs_file: str, blocking: bool, print_command_output=True) -> str:
        options = ''
        if blocking:
            options += '-Wblock=true'
        command_output = os.popen(f'qsub {options} {pbs_file}').read().strip()
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
            Which queue to submit to: devel, debug, normal, long,
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
    def __init__(self):
        """
        A fake PBS class for directly running commands while still calling as
        if it were a standard pbs driver.
        This can be used to seemless switch between modes where PBS jobs are
        launched for each "job", or using a FakePBS object when you don't want to
        launch a new pbs job for each "job", e.g., driving a script
        while already within the PBS job.
        """
        super().__init__()

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
