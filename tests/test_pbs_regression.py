import os
import filecmp
from typing import List
from pbs4py import PBS

test_directory = os.path.dirname(os.path.abspath(__file__))


def test_write_job_file_regression_check():
    golden_file = f'{test_directory}/pbs_test_files/golden0.pbs'
    queue_name = 'queue'
    ncpus_per_node = 5
    queue_node_limit = 10
    time = 24
    hashbang = '#!/usr/bin/bash'
    pbs = PBS(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
              queue_node_limit=queue_node_limit, time=time)
    pbs.hashbang = hashbang

    job_name = 'test_job'
    job_body = ['command1', 'command2']
    pbs_file = f'{test_directory}/test_output_files/test.pbs'
    pbs.write_job_file(pbs_file, job_name, job_body)

    assert filecmp.cmp(pbs_file, golden_file)


if __name__ == '__main__':
    test_write_job_file_regression_check()
