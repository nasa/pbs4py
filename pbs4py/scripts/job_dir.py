#!/usr/bin/env python
"""
A script to print the directory that a job is running in.

Can be used in combination with bash aliases to create a `qdir` alias for moving
to the directory a job is running in
```
qdirfun() { cd `job_dir.py $1`;}
alias qdir=qdirfun
```

Then in the shell instance you can do `qdir {job_id}` to move the job's run directory.
"""
import argparse
from pbs4py.job import PBSJob


def arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('job_id', help='The ID number of the job')
    return parser


def main():
    parser = arg_parser()

    args = parser.parse_args()
    job = PBSJob(args.job_id)
    print(job.workdir)


if __name__ == '__main__':
    main()
