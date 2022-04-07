import os
from typing import List


class PBSJob:
    def __init__(self, id: int):
        self.id = id
        self.read_properties_from_qstat()

    def read_properties_from_qstat(self):
        qstat_output = os.popen(f'qstat -xf {self.id}').read().split('\n')

        if self._is_a_known_job(qstat_output):
            self._parse_attributes_from_qstat_output(qstat_output)
        else:
            self._set_empty_attributes()

    def _is_a_known_job(self, qstat_output):
        return not 'Unknown Job Id' in qstat_output

    def _parse_attributes_from_qstat_output(self, qstat_output: List[str]):
        qstat_dict = self._convert_qstat_output_to_a_dictionary(qstat_output)

        self.name = qstat_dict['Job_Name']
        self.queue = qstat_dict['queue']
        self.state = qstat_dict['job_state']

        if 'model' in qstat_dict['Resource_List.select']:
            self.model = qstat_dict['Resource_List.select'].split('model=')[-1]
        else:
            self.model = ''

    def _convert_qstat_output_to_a_dictionary(self, qstat_output: List[str]) -> dict:
        qstat_dict = {}
        for line in qstat_output:
            if '=' in line:
                split_line = line.split('=', 1)
                key = split_line[0].strip()
                value = split_line[1].strip()
                qstat_dict[key] = value
        return qstat_dict

    def _set_empty_attributes(self):
        self.name = ''
        self.model = ''
        self.queue = ''
        self.state = ''
