#!/usr/bin/env python
"""
A script to delete active PBS jobs of the current user.
The list of jobs to be deleted can be filtered by id range, job name substring, and queue.
For safety, the default behavior is to show the user which jobs will be deleted and ask for confirmation
before any jobs are deleted.
"""
import os
import argparse
import re
from typing import List

from pbs4py.job import PBSJob


def arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--id_range',
                        nargs=2,
                        default=(-1, -1),
                        help='Delete jobs in a range of id numbers, [min id, max id]')
    parser.add_argument('--queue',
                        default='',
                        help='Delete jobs in a specific queue')
    parser.add_argument('--name',
                        default='',
                        help='Delete jobs in a specific string in the name')
    parser.add_argument('--confirm',
                        action='store_true',
                        dest='confirm',
                        help='Whether to prompt the user for confirmation of before deleting')
    parser.add_argument('--no-confirm', dest='confirm', action='store_false')
    parser.set_defaults(confirm=True)
    return parser


def get_active_jobs_for_user():
    user_name = os.environ.get('USER')
    qstat_output = os.popen(f'qstat -u {user_name}').read().split('\n')

    # remove header from qstat command
    qstat_output = qstat_output[3:]

    jobs = []
    for line in qstat_output:
        if line:
            id = int(re.match('\s*[0-9]+', line)[0])
            jobs.append(PBSJob(id))
    return jobs


def filter_jobs_to_delete_by_id_range(user_jobs: List[PBSJob], min_id: int, max_id: int):
    return [job for job in user_jobs if (job.id >= min_id and job.id <= max_id)]


def filter_jobs_to_delete_by_queue(user_jobs: List[PBSJob], queue: str):
    return [job for job in user_jobs if job.queue == queue]


def filter_jobs_to_delete_by_name_substring(user_jobs: List[PBSJob], name_substring: str):
    return [job for job in user_jobs if name_substring in job.name]


def delete_jobs(jobs: List[PBSJob]):
    for job in jobs:
        job.qdel(echo_command=True)


def print_jobs_that_will_be_deleted(jobs_to_delete: List[PBSJob]):
    print('Found the following jobs:')
    print('------------------------')
    for job in jobs_to_delete:
        print(f'Job: id = {job.id}, name = {job.name}, queue: {job.queue}')


def user_confirms():
    prompt = 'Delete these jobs? [y/n]'
    valid = {"yes": True, "y": True, "no": False, "n": False}

    while True:
        print(prompt)
        choice = input().lower()
        if choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' ", "(or 'y' or 'n').\n")


def main():
    parser = arg_parser()

    args = parser.parse_args()
    confirm = args.confirm
    min_id = int(args.id_range[0])
    max_id = int(args.id_range[1])
    queue = args.queue
    name_substring = args.name

    jobs_to_delete = get_active_jobs_for_user()

    if min_id > 0 and max_id > 0:
        print('Filtering by id range')
        jobs_to_delete = filter_jobs_to_delete_by_id_range(jobs_to_delete, min_id, max_id)
    if queue:
        print('Filtering by queue')
        jobs_to_delete = filter_jobs_to_delete_by_queue(jobs_to_delete, queue)
    if name_substring:
        print('Filtering by name')
        jobs_to_delete = filter_jobs_to_delete_by_name_substring(jobs_to_delete, name_substring)

    if len(jobs_to_delete) == 0:
        print(f'No active jobs found for user with specified filters')
        exit()

    if confirm:
        print_jobs_that_will_be_deleted(jobs_to_delete)
        if user_confirms():
            delete_jobs(jobs_to_delete)
        else:
            print('Skipping')

    else:
        delete_jobs(jobs_to_delete)


if __name__ == '__main__':
    main()
