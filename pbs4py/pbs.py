#!/usr/bin/env python
import os
from typing import List
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

        #: str: The name of the queue which goes on the "#PBS -N {queue_name}"
        #: line of the pbs header
        self.queue_name = queue_name

        #: int: The number of CPU cores per node.
        self.ncpus_per_node = ncpus_per_node

        #: int: The maximum number nodes allowed by the queue
        self.queue_node_limit = queue_node_limit

        #: int: The requested wall time for the pbs job(s) in hours
        self.time = time

        #: int: The number of mpi ranks per node. The default behavior is to set
        #: `mpiprocs_per_node = ncpus_per_node` where ncpus_per_node is
        #: the value given during instantiation. For standard MPI execution,
        #: For mpi+openMP, `mpiprocs_per_node` may be less than `ncpus_per_node`.
        self.mpiprocs_per_node = ncpus_per_node

        #: str or None: The processor model if it needs to be specified.
        #: The associated PBS header line is "#PBS -l select=#:ncpus=#:mpiprocs=#:model={model}"
        #: If left as `None`, the ":model={mode}" will not be added to the header line
        self.model = None

        #: str or None: The group for the group_list entry of the pbs header if necessary.
        #: The associated PBS header line is "#PBS -W group_list={group_list}"
        self.group_list = None

        #: str or None: Requested memory size on the select line. Need to include units in the str.
        #: The associated PBS header line is "#PBS -l select=#:mem={mem}"
        self.mem = None

        #: str: The hashbang line which sets the shell for the PBS script.
        #: If unset, the default is "#!/usr/bin/env bash".
        self.hashbang = '#!/usr/bin/env bash'

        #: str: The mpi execution command name: mpiexec, mpirun, mpiexec_mpt, etc.
        self.mpiexec = 'mpiexec'

        #: str: pbs -m mail options. 'e' at exit, 'b' at beginning, 'a' at abort
        self.mail_options = None

        #: str: pbs -M mail list. Who to email when mail_options are triggered
        self.mail_list = None

        #: str: Type of dependency if dependency active.
        #: Default is 'afterok' which only launches the new job if the previous one was successful.
        self.dependency_type = 'afterok'

        #: str: Profile file to source to top of PBS script
        self.profile_filename = profile_file

        #: int: number of compute nodes to request
        self.requested_number_of_nodes = 1

    @property
    def profile_filename(self):
        """
        The file to source at the start of the pbs script to set the environment.
        Typical names include "~/.profile", "~/.bashrc", and "~/.cshrc".
        The default profile filename is "~/.bashrc"
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
        The node of nodes to request. That is, the "select" number in the
        "#PBS -l select={number_of_nodes}:ncpus=40:mpiprocs=40".

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

    def write_pbs_file(self, pbs_file: str, job_name: str,
                       job_body: List[str], dependency: str = None):
        """
        Create a pbs script file

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
        with open(pbs_file, mode='w') as fh:
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

    def _create_select_line(self):
        select = f'select={self.requested_number_of_nodes}'
        ncpus = f'ncpus={self.ncpus_per_node}'
        mpiprocs = f'mpiprocs={self.mpiprocs_per_node}'

        select_line = f'#PBS -l {select}:{ncpus}:{mpiprocs}'
        if self.mem is not None:
            select_line += f':mem={self.mem}'
        if self.model is not None:
            select_line += f':model={self.model}'
        return select_line

    def _create_pbs_header(self, job_name, dependency=None):
        """
        Create the pbs job description block.

        Parameters
        ----------
        job_name: str
            The name of the job
        dependency: str
            PBS jobs or colon separated jobs that this one depends one.

        Returns
        -------
        pbs_header: list[str]
            List of strings representing the header
        """
        pbs_header = [self.hashbang,
                      f'#PBS -N {job_name}',
                      f'#PBS -q {self.queue_name}',
                      self._create_select_line(),
                      f'#PBS -l walltime={self.time}:00:00',
                      f'#PBS -o {job_name}_pbs.log',
                      '#PBS -j oe',  # join standard and error ouptut
                      '#PBS -r n']

        if self.group_list is not None:
            pbs_header.insert(2, '#PBS -W group_list=%s' % self.group_list)
        if self.mail_options is not None:
            pbs_header.append(f'#PBS -m {self.mail_options}')
        if self.mail_list is not None:
            pbs_header.append(f'#PBS -M {self.mail_list}')
        if dependency is not None:
            pbs_header.append(f'#PBS -W depend={self.dependency_type}:{dependency}')
        return pbs_header

    def _run_pbs_job(self, pbs_file, blocking):
        options = ''
        if blocking:
            options += '-Wblock=true'
        return os.popen(f'qsub {options} {pbs_file}').read().strip()

    # Alternate constructors for NASA HPC queues
    @classmethod
    def k4(cls, time=72, profile_file='~/.bashrc'):
        """
        The K4 queues including K4-standard-512. Skylake nodes.
        """
        return cls(queue_name='K4-route', ncpus_per_node=40,
                   queue_node_limit=16, time=time,
                   profile_file=profile_file)

    @classmethod
    def k3(cls, time=72, profile_file='~/.bashrc'):
        """
        The K3 queues including K4-standard-512. Skylake nodes.
        """
        return cls(queue_name='K3-route', ncpus_per_node=16,
                   queue_node_limit=40, time=time,
                   profile_file=profile_file)

    @classmethod
    def k3a(cls, time=72, profile_file='~/.bashrc'):
        return cls(queue_name='K3a-route', ncpus_per_node=16,
                   queue_node_limit=25, time=time,
                   profile_file=profile_file)

    @classmethod
    def nas(cls, group_list, proc_type='broadwell', queue_name='long',
            time=72, profile_file='~/.bashrc'):

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
        else:  # default to sandy bridge
            print('Unknown NAS processor. Defaulting to Sandy Bridge')
            ncpus_per_node = 20
            model = 'san'

        pbs = cls(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
                  queue_node_limit=1e99, time=time, profile_file=profile_file)

        pbs.group_list = group_list
        pbs.model = model
        return pbs


class FakePBS(PBS):
    def __init__(self):
        """
        A fake PBS class for directly running commands while still calling as
        if it were a standard pbs driver.
        """
        super().__init__()

    def launch(self, job_name: str, job_body: List[str], blocking: bool = True):
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

        Returns
        -------
        pbs_command_output: str
            Empty string but returning something to match true PBS launch output
        """
        for line in job_body:
            print(line)
            os.system(line)
        return ''
