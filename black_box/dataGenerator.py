import os
import subprocess
import pandas
import time
from datetime import datetime

def runLSMTree(is_compressed, query_path):
    os.chdir('../lsm_tree_compression')
    bash_command = "./server {is_compressed} {query_path}".format(is_compressed=is_compressed, query_path=query_path)
    #print(bash_command)

    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    print(output, error)
    time = float(output.decode('utf-8').split('\n')[3].split(':')[1])
    data_path = '../lsm_tree_compression/enc/'
    data_size = sum(os.path.getsize(data_path + f) for f in os.listdir(data_path))
    
    bash_command = "rm -rf data enc"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    bash_command = "mkdir enc"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    bash_command = "mkdir data"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    return time

def createData(size, numberQueries, dataDistribution):
    os.chdir('../lsm_tree_compression')
    bash_command = """./gen -N {size} -queries {numberQueries} -keyDistribution {dataDistribution} -valueDistribution 1 -readPercentage 100""".format(size=size, numberQueries=numberQueries, dataDistribution=dataDistribution)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    return None

def compareCompressedToUncompressed(query_path):
    compressed_time = runLSMTree(0, query_path)
    uncompressed_time = runLSMTree(3, query_path)

    return compressed_time, uncompressed_time

def createDataParameters(numEntries):
    #TODO: parametrize this and make it more random
    size = [10000, 10000, 10000]
    numberQueries = [10000, 10000, 10000]
    dataDistribution = [0,1,2]
    currSize = 10000

    for i in range(1, numEntries):
        currSize += (int)(currSize/8)
        for j in range(3):
            size.append(currSize)
            numberQueries.append(10000)
            dataDistribution.append(j)

    return size, numberQueries, dataDistribution

def main():
    numEntries = 50
    query_path = "queries.dsl"
    size, numberQueries, dataDistribution = createDataParameters(numEntries)
    #print(size, numberQueries, dataDistribution)
    compressed_times = []
    uncompressed_times = []
    y = []
    err_count = 0

    for i in range(len(size)):
        print("Entry: ", i, "Size: ", size[i], numberQueries[i], dataDistribution[i])
        createData(size[i], numberQueries[i], dataDistribution[i])
        try:
            compressed_time, uncompressed_time = compareCompressedToUncompressed(query_path)
            compressed_times.append(compressed_time)
            uncompressed_times.append(uncompressed_time)
            y.append(compressed_time > uncompressed_time)
        except IndexError:
            print("Some Error")
            err_count += 1

    print("Number Errors: ", err_count)
    now = datetime.now()
    file_name = now.strftime("%d%m%Y%H%M%S")
    df = pandas.DataFrame(list(zip(size, numberQueries, dataDistribution, compressed_times, uncompressed_times, y)), 
                            columns=['size', 'numberQueries', 'dataDistribution', 'compressed_time', 'uncompressed_time', 'y'])
    df.to_csv('../data/' + file_name + '.csv')

if __name__ == '__main__':
    main()
