import os
import subprocess
import time
from datetime import datetime
import csv

def runLSMTree(num_reads, num_writes, leniency):
    os.chdir('../lsm_tree_compression')
    bash_command = "./server 1 queries.dsl {read_amount} {write_amount} {leniency}".format(read_amount=num_reads, write_amount=num_writes, leniency=leniency)

    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
    load_time = float(output.decode('utf-8').split('\n')[1].split(':')[1])
    read_time = float(output.decode('utf-8').split('\n')[-3].split(':')[1])

    # Get the sizes of the files and the compression ratio
    data_path = '../lsm_tree_compression/enc/'
    data_size = get_size(data_path)

    print(output, error, data_size, load_time, read_time)
    bash_command = "rm -rf data enc"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    bash_command = "mkdir enc"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    bash_command = "mkdir data"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    return load_time, read_time, data_size

def createData(paramater_list):
    os.chdir('../lsm_tree_compression')
    bash_command = """./gen -N {size} -queries {numberQueries} -keyDistribution {dataDistribution} -valueDistribution {keyDistribution} -readPercentage {readPercentage}""".format(size=paramater_list[0], numberQueries=paramater_list[1], dataDistribution=paramater_list[3], keyDistribution=paramater_list[4], readPercentage=paramater_list[2])
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    return None

def parameters(readPercentage):
    paramater_list = []
    N = 100000
    queries = 10
    read = readPercentage
    # What do I Need:
    # N: 1,000,000
    # queries: 100,000
    # keyDistribution
    # valueDistribution
    # readPercentage 
    for i in range(3):
        for j in range(2):
            l = [N, queries, read, i, j]
            paramater_list.append(l)

    return paramater_list   

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    
    return total_size

def main():
    readPercentage = 100
    curr_parameters = parameters(readPercentage)
    leniencies = [1, 1.3, 1.6, 1.9, 2.2, 2.5, 2.8, 3.1, 3.4, 3.7, 4.0]

    read_times = []
    load_times = []
    sizes = []
    keyDistrib = []
    valueDistrib = []
    all_leniencies = []

    err_count = 0

    for l in curr_parameters:
        createData(l)
        for leniency in leniencies:
            num_read = readPercentage * 10 * 0.01
            num_write = 10 - num_read

            try:
                load, read, size = runLSMTree(num_read, num_write, leniency)
                read_times.append(read)
                load_times.append(load)
                sizes.append(size)
                keyDistrib.append(l[3])
                valueDistrib.append(l[4])
                all_leniencies.append(leniency)
            except IndexError:
                err_count += 1
                print("Index Error")
    
    print("Number Errors: ", err_count)
    file_name = '../data/results_' + str(readPercentage) + '.csv'
    columns = ['keyDistribution', 'valueDistribution', 'load_times', 'query_times', 'sizes', 'leniency']
    data = list(zip(keyDistrib, valueDistrib, load_times, read_times, sizes, all_leniencies))

    with open(file_name, 'a') as outcsv: 
        writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        writer.writerow(columns)
        for item in data:
            #Write item to outcsv
            writer.writerow(item)

    print(file_name)

if __name__ == "__main__":
    main()
    

            






