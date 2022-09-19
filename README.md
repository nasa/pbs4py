# Description

pbs4py is a Python module for automating submission of compute jobs on High Performance
Computing clusters, such as those that use the Portable Batch System (PBS).
It includes pre-configured launchers for common NASA HPC systems: the Langley K cluster
and NASA Advanced Supercomputing (NAS) systems.

Examples uses are uncertainty quantification where many jobs are submitted
simultaneously or optimization where sequences of jobs need to scripted.

pbs4py also includes scripts for performing tasks associated with PBS jobs
such as a script when given a job number will print the directory from which it was launched
and a script that can delete multiple jobs based on filters.


# Documentation
The pbs4py documentation is generated from the source code with [Sphinx](https://www.sphinx-doc.org/en/master/).
Once you have installed pbs4py, the documentation is built by running `make html` in the docs directory.
The generated documentation will be in `docs/build/html`.


# Quick Start

After installation,

On the K cluster:
```python
from pbs4py import PBS
pbs = PBS.k4()
pbs.requested_number_of_nodes = 1
pbs.launch(job_name='example_job',job_body=['echo "Hello World"'])
```

On NAS:
```python
from pbs4py import PBS
group = 'a1111' # your project ID to charge here
pbs = PBS.nas(group, proc_type='san', queue='devel', time=1)
pbs.launch(job_name='example_job',job_body=['echo "Hello World"'])
```
