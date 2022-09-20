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
[Documentation is hosted using Github Pages](https://nasa.github.io/pbs4py/)

The pbs4py documentation is generated from the source code with [Sphinx](https://www.sphinx-doc.org/en/master/).
Once you have installed pbs4py, the documentation is built by running `make html` in the docs directory.
The generated documentation will be in `docs/build/html`.

# Installation
pbs4py can be installed with

```
pip install pbs4py
```

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

# License Notices and Disclaimers
Notices:
Copyright 2022 United States Government as represented by the Administrator of
the National Aeronautics and Space Administration. No copyright is claimed in
the United States under Title 17, U.S. Code. All Other Rights Reserved.

Third Party Software:

This software calls the following third party software, which is subject to the
terms and conditions of its licensor, as applicable at the time of licensing.
Third party software is not bundled with this software, but may be available
from the licensor. License hyperlinks are provided here for information purposes
only: numpy, BSD 3-Clause "New" or "Revised" License,
https://github.com/numpy/numpy/blob/main/LICENSE.txt.

Disclaimers
No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF
ANY KIND, EITHER EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED
TO, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY
IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE ERROR
FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE
SUBJECT SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN
ENDORSEMENT BY GOVERNMENT AGENCY OR ANY PRIOR RECIPIENT OF ANY RESULTS,
RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY OTHER APPLICATIONS
RESULTING FROM USE OF THE SUBJECT SOFTWARE.  FURTHER, GOVERNMENT AGENCY
DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF
PRESENT IN THE ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."

Waiver and Indemnity:  RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE
UNITED STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY
PRIOR RECIPIENT.  IF RECIPIENT'S USE OF THE SUBJECT SOFTWARE RESULTS IN ANY
LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES ARISING FROM SUCH USE,
INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM, RECIPIENT'S USE
OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR
RECIPIENT, TO THE EXTENT PERMITTED BY LAW.  RECIPIENT'S SOLE REMEDY FOR ANY SUCH
MATTER SHALL BE THE IMMEDIATE, UNILATERAL TERMINATION OF THIS AGREEMENT.
