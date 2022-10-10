import os
import pytest
from typing import List
from pbs4py import PBS

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


def check_list_of_strings(actual: List[str], expected: List[str]):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a == e


@pytest.fixture
def pbs_header_test():
    queue_name = 'queue'
    ncpus_per_node = 5
    queue_node_limit = 10
    time = 24
    hashbang = '#!/usr/bin/tcsh'
    pbs_header_test = PBS(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
                          queue_node_limit=queue_node_limit, time=time,
                          profile_file=test_profile)
    pbs_header_test.hashbang = hashbang
    pbs_header_test.requested_number_of_nodes = 2
    return pbs_header_test


def test_walltime_line(pbs_header_test: PBS):
    pbs_header_test.time = 5
    line = pbs_header_test._create_walltime_line_of_header()
    assert line == '#PBS -l walltime=5:00:00'


def test_log_line(pbs_header_test: PBS):
    job_name = 'test'
    line = pbs_header_test._create_log_name_line_of_header(job_name)
    assert line == '#PBS -o test_pbs.log'


def test_join_output_line(pbs_header_test: PBS):
    line = pbs_header_test._create_header_line_to_join_standard_and_error_output()
    assert line == '#PBS -j oe'


def test_rerunnable_line(pbs_header_test: PBS):
    line = pbs_header_test._create_header_line_to_set_that_job_is_not_rerunnable()
    assert line == '#PBS -r n'


def test_select_line_with_no_model_or_mem_defined(pbs_header_test: PBS):
    header = pbs_header_test._create_select_line_of_header()
    expected = "#PBS -l select=2:ncpus=5:mpiprocs=5"
    assert header == expected


def test_select_line_with_model_defined(pbs_header_test: PBS):
    pbs_header_test.model = 'bro'
    header = pbs_header_test._create_select_line_of_header()
    expected = "#PBS -l select=2:ncpus=5:mpiprocs=5:model=bro"
    assert header == expected


def test_select_line_with_mem_defined(pbs_header_test: PBS):
    pbs_header_test.mem = '245gb'
    header = pbs_header_test._create_select_line_of_header()
    expected = "#PBS -l select=2:ncpus=5:mpiprocs=5:mem=245gb"
    assert header == expected


def test_pbs_header_with_group_name_not_defined(pbs_header_test: PBS):
    header = pbs_header_test._create_group_list_header_line()
    expected = []
    check_list_of_strings(header, expected)


def test_pbs_header_with_group_name_defined(pbs_header_test: PBS):
    pbs_header_test.group_list = 'n1337'
    header = pbs_header_test._create_group_list_header_line()
    expected = ["#PBS -W group_list=n1337"]
    check_list_of_strings(header, expected)


def test_pbs_header_email_option(pbs_header_test: PBS):
    pbs_header_test.mail_options = 'be'
    pbs_header_test.mail_list = 'kevin@nasa.gov'
    header = pbs_header_test._create_mail_options_header_lines()
    expected = ['#PBS -m be', '#PBS -M kevin@nasa.gov']
    check_list_of_strings(header, expected)


def test_job_line_of_header(pbs_header_test: PBS):
    job_name = 'test_job'
    assert '#PBS -N test_job' == pbs_header_test._create_job_line_of_header(job_name)


def test_queue_line_of_header(pbs_header_test: PBS):
    pbs_header_test.queue_name = 'K4-standard'
    assert '#PBS -q K4-standard' == pbs_header_test._create_queue_line_of_header()


def test_array_range_line_of_header_default_is_off(pbs_header_test: PBS):
    assert [] == pbs_header_test._create_array_range_header_line()


def test_array_range_line_of_header(pbs_header_test: PBS):
    pbs_header_test.array_range = '1-24'
    assert ['#PBS -J 1-24'] == pbs_header_test._create_array_range_header_line()
    pbs_header_test.array_range = None
    assert [] == pbs_header_test._create_array_range_header_line()


def test_job_dependency_line_of_header(pbs_header_test: PBS):
    assert [] == pbs_header_test._create_job_dependencies_header_line(dependency=None)
    assert [
        '#PBS -W depend=afterok:a.1234'] == pbs_header_test._create_job_dependencies_header_line(dependency='a.1234')

    pbs_header_test.dependency_type = 'before'
    assert [
        '#PBS -W depend=before:b.4321'] == pbs_header_test._create_job_dependencies_header_line(dependency='b.4321')
