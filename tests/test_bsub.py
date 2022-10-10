import os
import pytest
from typing import List
from pbs4py.bsub import BSUB

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


def check_list_of_strings(actual: List[str], expected: List[str]):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a == e


@pytest.fixture
def bsub_header_test():
    project = 'ard149'
    ngpu = 5
    time = 24
    hashbang = '#!/usr/bin/tcsh'
    bsub_header_test = BSUB(project, ngpu_per_node=ngpu, time=time,
                            profile_filename=test_profile)
    bsub_header_test.hashbang = hashbang
    bsub_header_test.requested_number_of_nodes = 2
    return bsub_header_test


def test_walltime_line(bsub_header_test: BSUB):
    bsub_header_test.time = 5
    line = bsub_header_test._create_wall_time_line_of_header()
    assert line == '#BSUB -W 5:00'


def test_number_of_nodes_line(bsub_header_test: BSUB):
    line = bsub_header_test._create_number_of_nodes_line_of_header()
    assert line == '#BSUB -nnodes 2'


def test_job_line(bsub_header_test: BSUB):
    job_name = 'test'
    line = bsub_header_test._create_job_name_line_of_header(job_name)
    assert line == '#BSUB -J test'


def test_project_line(bsub_header_test: BSUB):
    bsub_header_test.project = 'ard149'
    line = bsub_header_test._create_project_line_of_header()
    assert line == '#BSUB -P ard149'


def test_mail_header(bsub_header_test: BSUB):
    bsub_header_test.mail_when_complete = False
    header = bsub_header_test._create_mail_header_line()
    check_list_of_strings(header, [])

    bsub_header_test.mail_when_complete = True
    header = bsub_header_test._create_mail_header_line()
    check_list_of_strings(header, ['#BSUB -N'])


def test_job_dependency_header(bsub_header_test: BSUB):
    header = bsub_header_test._create_job_dependency_header_line(None)
    check_list_of_strings(header, [])

    header = bsub_header_test._create_job_dependency_header_line('1234')
    check_list_of_strings(header, ['#BSUB -w ended(1234)'])


def test_parse_job_id_from_bsub_output(bsub_header_test: BSUB):
    output = 'Job <1983914> is submitted to default queue <batch>.'
    id = bsub_header_test._parse_job_id_out_of_bsub_output(output)
    assert id == 1983914


def test_create_command(bsub_header_test: BSUB):
    bsub_header_test.ngpu_per_node = 3
    bsub_header_test.requested_number_of_nodes = 2
    command = bsub_header_test.create_mpi_command('a.out', 'dog', openmp_threads=2)
    assert command == 'jsrun -n 6 -a 1 -c 2 -g 1 a.out &> dog.out'
