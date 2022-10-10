import os
from pbs4py import FakePBS

test_directory = os.path.dirname(os.path.abspath(__file__))


def test_fakePBS():
    pbs = FakePBS(profile_file=f'{test_directory}/testing_bashrc')
    job_name = 'test'

    file = 'fake_file.txt'
    assert not os.path.isfile(f'{test_directory}/{file}')
    job_body = [f'touch {test_directory}/{file}']
    pbs.launch(job_name, job_body)
    assert os.path.isfile(f'{test_directory}/{file}')
    os.system(f'rm {test_directory}/{file}')
