import os
from typing import List, Union


class PBSJob:
    def __init__(self, id: int):
        """
        A class for querying information and managing a particular submitted
        pbs job. For the id number in the constructor, the qstat command will
        be used to populate the attributes of the job.

        Parameters
        ----------
        id:
            The id number of the job
        """

        #: The ID number of the job
        self.id: int = id

        #: The name of the job
        self.name: str = ''

        #: The model attribute on the select line from the job submission
        self.model: str = ''

        #: The queue which this job was submitted to
        self.queue: str = ''

        #: Whether the job is queued, running, or finished
        self.state: str = ''

        #: The value of $PBS_O_WORKDIR
        self.workdir: str = ''

        self.read_properties_from_qstat()

    def read_properties_from_qstat(self):
        """
        Use qstat to get the current attributes of this job
        """
        qstat_output = self._run_qstat_to_get_full_job_attributes()
        if self._is_a_known_job(qstat_output):
            self._parse_attributes_from_qstat_output(qstat_output)
        else:
            self._set_empty_attributes()

    def qdel(self, echo_command: bool = True) -> str:
        """
        Call qdel to delete this job

        Parameters
        ----------
        echo_command:
            Whether to print the command before running it

        Returns
        -------
        command_output: str
            The output of the shell command
        """
        command = f'qdel {self.id}'
        if echo_command:
            print(command)
        return os.popen(command).read()

    def _run_qstat_to_get_full_job_attributes(self) -> Union[List[str], str]:
        return os.popen(f'qstat -xf {self.id}').read().split('\n')

    def _is_a_known_job(self, qstat_output):
        return not 'Unknown Job Id' in qstat_output

    def _parse_attributes_from_qstat_output(self, qstat_output: List[str]):
        qstat_dict = self._convert_qstat_output_to_a_dictionary(qstat_output)

        self.name: str = qstat_dict['Job_Name']
        self.queue: str = qstat_dict['queue']
        self.state: str = qstat_dict['job_state']
        self.workdir = self._parse_workdir(qstat_dict)

        if 'model' in qstat_dict['Resource_List.select']:
            self.model = qstat_dict['Resource_List.select'].split('model=')[-1]
        else:
            self.model = ''

    def _set_empty_attributes(self):
        self.name = ''
        self.model = ''
        self.queue = ''
        self.state = ''
        self.workdir = ''

    def _parse_workdir(self, qstat_dict: dict) -> str:
        return qstat_dict['Variable_List'].split('PBS_O_WORKDIR=')[-1].split(',')[0]

    def _convert_qstat_output_to_a_dictionary(self, qstat_output: List[str]) -> dict:
        qstat_dict = {}
        for line in qstat_output:
            if '=' in line:
                split_line = line.split('=', 1)
                key = split_line[0].strip()
                value = split_line[1].strip()
                qstat_dict[key] = value
        return qstat_dict
