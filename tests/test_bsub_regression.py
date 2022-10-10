import os
import filecmp
from pbs4py.bsub import BSUB

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
    golden_mod = f'{test_directory}/test_output_files/golden_mod.lsf'
    with open(golden_mod, 'w') as fh:
        fh.writelines(golden_file_with_profile)
    return golden_mod


def test_write_job_file_regression_check():
    golden_file = f'{test_directory}/pbs_test_files/golden0.lsf'
    project = 'ard149'
    ngpu = 5
    time = 24
    shell = 'tcsh'
    bsub = BSUB(project, time=time, profile_filename=test_profile)
    bsub.shell = shell
    bsub.requested_number_of_nodes = 2

    job_name = 'test_job'
    job_body = ['command1', 'command2']
    bsub_file = f'{test_directory}/test_output_files/test.lsf'
    bsub.write_job_file(bsub_file, job_name, job_body)

    golden_mod = modify_golden_file_to_have_right_path_for_profile(
        golden_file, bsub.profile_filename)

    assert filecmp.cmp(bsub_file, golden_mod)


if __name__ == '__main__':
    test_write_job_file_regression_check()
