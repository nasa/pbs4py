from pbs4py import PBS

k4 = PBS.k4(time=48)
k4.mpiexec = 'mpiexec_mpt'
k4.requested_number_of_nodes = 2

fun3d_command = 'nodet_mpi'
fun3d_mpi_command = k4.create_mpi_command(fun3d_command, 'dog', openmp_threads=20)

# commands that will be run in the pbs script
pbs_commands = [fun3d_mpi_command]

# submit and wait for job to finish before continuing script
k4.launch('omp_job', pbs_commands)
