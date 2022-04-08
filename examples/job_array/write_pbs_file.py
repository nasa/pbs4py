from pbs4py import PBS

k4 = PBS.k4(time=48)
k4.mpiexec = 'mpiexec_mpt'
k4.requested_number_of_nodes = 1

k4.array_range = '1-4'

command_list = [f'echo "Array job index = ${{PBS_ARRAY_INDEX}}"']

k4.write_job_file('test_array.pbs', 'test_array', command_list)
