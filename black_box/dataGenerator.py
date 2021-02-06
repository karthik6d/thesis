import os
import subprocess
import time
from datetime import datetime
import csv

def runLSMTree(is_compressed, query_path):
    os.chdir('../lsm_tree_compression')
    bash_command = "./server {is_compressed} {query_path}".format(is_compressed=is_compressed, query_path=query_path)
    #print(bash_command)

    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
    load_time = float(output.decode('utf-8').split('\n')[1].split(':')[1])
    read_time = float(output.decode('utf-8').split('\n')[-3].split(':')[1])

    # Get the sizes of the files and the compression ratio
    if is_compressed != 0:
        data_path = '../lsm_tree_compression/enc/'
        data_size = get_size(data_path)
    else:
        data_path = '../lsm_tree_compression/data/'
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

def createData(size, numberQueries, dataDistribution, keyDistribution, readPercentage=100):
    os.chdir('../lsm_tree_compression')
    bash_command = """./gen -N {size} -queries {numberQueries} -keyDistribution {dataDistribution} -valueDistribution {keyDistribution} -readPercentage {readPercentage}""".format(size=size, numberQueries=numberQueries, dataDistribution=dataDistribution, keyDistribution=keyDistribution, readPercentage=readPercentage)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    return None

def compareCompressedToUncompressed(query_path):
    uncompressed_load_time, uncompressed_read_time, uncompressed_size = runLSMTree(0, query_path)
    snappy_load_time, snappy_read_time, snappy_size = runLSMTree(1, query_path)
    simd_load_time, simd_read_time, simd_size = runLSMTree(2, query_path)
    rle_load_time, rle_read_time, rle_size = runLSMTree(3, query_path)
    zlib_load_time, zlib_read_time, zlib_size = runLSMTree(4, query_path)
    zstandard_load_time, zstandard_read_time, zstandard_size = runLSMTree(5, query_path)

    sizes = {"uncompressed": uncompressed_size, "snappy": snappy_size, "simd": simd_size, "rle": rle_size, "zlib": zlib_size, "zstandard": zstandard_size}
    read_times = {"uncompressed": uncompressed_read_time, "snappy": snappy_read_time, "simd": simd_read_time, "rle": rle_read_time, "zlib": zlib_read_time, "zstandard": zstandard_read_time}
    load_times = {"uncompressed": uncompressed_load_time, "snappy": snappy_load_time, "simd": simd_load_time, "rle": rle_load_time, "zlib": zlib_load_time, "zstandard": zstandard_load_time}
    return sizes, read_times, load_times

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    
    return total_size

def createDataParameters(numEntries):
    #TODO: parametrize this and make it more random
    size = [10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000]
    numberQueries = [10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000]
    dataDistribution = [0,0,1,1,2,2,0,0,1,1,2,2]
    keyDistribution = [0,1,0,1,0,1,0,1,0,1,0,1]
    readPercentage = [100, 50, 100, 50, 100, 50, 100, 50, 100, 50, 100, 50]
    currSize = 10000

    for _ in range(1, numEntries):
        currSize += (int)(currSize/8)
        for j in range(3):
            for k in range(2):
                size.append(currSize)
                numberQueries.append(10000)
                dataDistribution.append(j)
                keyDistribution.append(k)
                readPercentage.append(100)
                size.append(currSize)
                numberQueries.append(10000)
                dataDistribution.append(j)
                keyDistribution.append(k)
                readPercentage.append(50)


    return size, numberQueries, dataDistribution, keyDistribution, readPercentage

def main():
    numEntries = 50
    query_path = "queries.dsl"
    size, numberQueries, dataDistribution, keyDistribution, readPercentage = createDataParameters(numEntries)
    print(size, numberQueries, dataDistribution, keyDistribution, readPercentage)
    # Output Data
    snappy_read_times = []
    simd_read_times = []
    rle_read_times = []
    zlib_read_times = []
    zstandard_read_times = []
    uncompressed_read_times = []

    snappy_load_times = []
    simd_load_times = []
    rle_load_times = []
    zlib_load_times = []
    zstandard_load_times = []
    uncompressed_load_times = []

    snappy_sizes = []
    simd_sizes = []
    rle_sizes = []
    zlib_sizes = []
    zstandard_sizes = []
    uncompressed_sizes = []

    err_count = 0

    for i in range(len(size)):
        print("Entry: ", i, "Size: ", size[i], numberQueries[i], dataDistribution[i], keyDistribution[i], readPercentage[i])
        createData(size[i], numberQueries[i], dataDistribution[i], keyDistribution[i], readPercentage[i])
        try:
            sizes, read_times, load_times = compareCompressedToUncompressed(query_path)
            # Add the read times
            snappy_read_times.append(read_times['snappy'])
            simd_read_times.append(read_times['simd'])
            rle_read_times.append(read_times['rle'])
            zlib_read_times.append(read_times['zlib'])
            zstandard_read_times.append(read_times['zstandard'])
            uncompressed_read_times.append(read_times['uncompressed'])
            # Add the load times
            snappy_load_times.append(load_times['snappy'])
            simd_load_times.append(load_times['simd'])
            rle_load_times.append(load_times['rle'])
            zlib_load_times.append(load_times['zlib'])
            zstandard_load_times.append(load_times['zstandard'])
            uncompressed_load_times.append(load_times['uncompressed'])
            # Add the sizes
            snappy_sizes.append(sizes['snappy'])
            simd_sizes.append(sizes['simd'])
            rle_sizes.append(sizes['rle'])
            zlib_sizes.append(sizes['zlib'])
            zstandard_sizes.append(sizes['zstandard'])
            uncompressed_sizes.append(sizes['uncompressed'])

        except IndexError:
            print("Some Error")
            err_count += 1

    print("Number Errors: ", err_count)
    now = datetime.now()
    file_name = '../data/' + now.strftime("%d%m%Y%H%M%S") + '.csv'
    columns = ['size', 'numberQueries', 'keyDistribution', 'valueDistribution', 'snappy_read_times', 'simd_read_times', 'rle_read_times', 'zlib_read_times', 'zstandard_read_times',
                                        'uncompressed_read_times', 'snappy_load_times', 'simd_load_times', 'rle_load_times',
                                        'zlib_load_times', 'zstandard_load_times', 'uncompressed_load_times',
                                        'snappy_sizes', 'simd_sizes', 'rle_sizes', 'zlib_sizes', 'zstandard_sizes', 'uncompressed_sizes', 'readPercentage']
    data = list(zip(size, numberQueries, dataDistribution, keyDistribution, snappy_read_times, simd_read_times,
                                    rle_read_times, zlib_read_times, zstandard_read_times, uncompressed_read_times, 
                                    snappy_load_times, simd_load_times, rle_load_times, zlib_load_times, zstandard_load_times, uncompressed_load_times,
                                    snappy_sizes, simd_sizes, rle_sizes, zlib_sizes, zstandard_sizes, uncompressed_sizes, readPercentage))

    with open(file_name, 'a') as outcsv: 
        writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        writer.writerow(columns)
        for item in data:
            #Write item to outcsv
            writer.writerow(item)

    print(file_name)

if __name__ == '__main__':
    main()
