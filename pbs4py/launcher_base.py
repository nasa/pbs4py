import os
from typing import List


class Launcher:
    def __init__(self):

        #: The hashbang line which sets the shell for the PBS script.
        #: If unset, the default is ``#!/usr/bin/env {self.shell}``.
        self.hashbang: str = None

        #: The shell flavor to use in the PBS job
        self.shell = 'bash'

        #: The number of compute nodes requested
        self.requested_number_of_nodes: int = 1

        # these are properties that users typically don't need to set, but are
        # specific to each queueing software
        self.workdir_env_variable: str = ''
        self.profile_filename: str = ''
        self.batch_file_extension: str = ''

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

    def create_mpi_command(self, command: str,
                           output_root_name: str,
                           openmp_threads: int = None) -> str:
        raise NotImplementedError('Launcher must implement a create_mpi_command method')

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
            if self.profile_filename != '':
                fh.write('source %s\n' % self.profile_filename)

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
        if self.shell == 'tcsh':
            return f'>& {output_filename}'
        else:
            return f'&> {output_filename}'
