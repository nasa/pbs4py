from setuptools import setup, find_packages

__package_name__ = "pbs4py"
__package_version__ = "0.0.2"

setup(
    name=__package_name__,
    version=__package_version__,
    description=("PBS scripting utilities"),
    author="Kevin Jacobson",
    author_email="kevin.e.jacobson@nasa.gov",
    zip_safe=False,
    packages=find_packages(),
    scripts=['pbs4py/scripts/qdel_user_jobs.py',
             'pbs4py/scripts/job_dir.py'],
    install_requires=['numpy']
)
