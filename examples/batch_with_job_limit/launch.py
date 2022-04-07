import os
from pbs4py import PBS, PBSBatch, BatchJob


pbs = PBS.k3a()
pbs.requested_number_of_nodes = 1

jobs = []
for ijob in range(10):
    name = f'sample{ijob}'
    commands = [f'sleep {ijob*60}',
                f'cat {name}.txt']
    jobs.append(BatchJob(name, commands))

batch = PBSBatch(pbs, jobs)
batch.create_directories()

for job in jobs:
    # use job as context manager to enter directory with the name of job.name and write a file
    with job:
        os.system(f'echo "hello world" > {job.name}.txt')

batch.launch_jobs_with_limit(max_jobs_at_a_time=3)
