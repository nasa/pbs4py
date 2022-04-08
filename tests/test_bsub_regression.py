import os
import filecmp
from pbs4py.bsub import BSUB

test_directory = os.path.dirname(os.path.abspath(__file__))


def test_write_job_file_regression_check():
    golden_file = f'{test_directory}/pbs_test_files/golden0.lsf'
    project = 'ard149'
    ngpu = 5
    time = 24
    hashbang = '#!/usr/bin/tcsh'
    bsub = BSUB(project, time=time)
    bsub.hashbang = hashbang
    bsub.requested_number_of_nodes = 2

    job_name = 'test_job'
    job_body = ['command1', 'command2']
    bsub_file = f'{test_directory}/test_output_files/test.lsf'
    bsub.write_job_file(bsub_file, job_name, job_body)

    assert filecmp.cmp(bsub_file, golden_file)


if __name__ == '__main__':
    test_write_job_file_regression_check()
