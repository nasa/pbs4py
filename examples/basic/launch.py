from pbs4py import PBS

k4 = PBS.k4(time=48)
k4.mpiexec = 'mpiexec_mpt'
k4.requested_number_of_nodes = 1

fun3d_command = 'nodet_mpi --gamma 1.14'
fun3d_mpi_command = k4.create_mpi_command(fun3d_command, 'dog')

# commands that will be run in the pbs script
pbs_commands = ['echo Start', fun3d_mpi_command, 'echo Done']

# submit and move on
k4.launch('test_job', pbs_commands, blocking=False)

# submit and wait for job to finish before continuing script
k4.launch('blocking_job', pbs_commands)
