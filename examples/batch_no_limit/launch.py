import os
from pbs4py import PBS, BatchJob, PBSBatch


pbs = PBS.k3()
pbs.requested_number_of_nodes = 1

jobs = []
for ijob in range(10):
    name = f'sleep{ijob}'
    commands = [f'sleep {ijob*10}',
                f'cat {name}.txt']
    jobs.append(BatchJob(name, commands))

batch = PBSBatch(pbs, jobs)
batch.create_directories()

for job in jobs:
    # use job as context manager to enter directory with the name of job.name and write a file
    with job:
        os.system(f'echo "hello world" > {job.name}.txt')

batch.launch_all_jobs(wait_for_jobs_to_finish=True)
print('Done.')
