import os
import filecmp
from pbs4py import PBS

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


def modify_golden_file_to_have_right_path_for_profile(golden_file: str, profile_filename: str):
    with open(golden_file, 'r') as fh:
        golden_file_contents = fh.readlines()
    golden_file_with_profile = []
    for line in golden_file_contents:
        if line == 'source_line\n':
            golden_file_with_profile.append(f'source {profile_filename}\n')
        else:
            golden_file_with_profile.append(line)
    golden_mod = f'{test_directory}/test_output_files/golden_mod.pbs'
    with open(golden_mod, 'w') as fh:
        fh.writelines(golden_file_with_profile)
    return golden_mod


def test_write_job_file_regression_check():
    golden_file = f'{test_directory}/pbs_test_files/golden0.pbs'
    queue_name = 'queue'
    ncpus_per_node = 5
    queue_node_limit = 10
    time = 24
    hashbang = '#!/usr/bin/bash'
    pbs = PBS(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
              queue_node_limit=queue_node_limit, time=time,
              profile_file=test_profile)
    pbs.hashbang = hashbang

    job_name = 'test_job'
    job_body = ['command1', 'command2']
    pbs_file = f'{test_directory}/test_output_files/test.pbs'
    pbs.write_job_file(pbs_file, job_name, job_body)

    golden_mod = modify_golden_file_to_have_right_path_for_profile(
        golden_file, pbs.profile_filename)

    assert filecmp.cmp(pbs_file, golden_mod)


if __name__ == '__main__':
    test_write_job_file_regression_check()
