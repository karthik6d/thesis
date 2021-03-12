from random import randint
import numpy
import struct

def uniform(size):
    const_num = randint(1, 100000)
    return [const_num] * size

def sequential(size):
    l = []

    for i in range(size):
        l.append(i)
    
    return l

def normal(size):
    l = numpy.random.normal(loc=100, scale=10, size=(size))
    x = []
    for num in l:
        x.append(int(num))

    return x

def random(size):
    lower_bound = 1
    upper_bound = 1000
    l = []

    for i in range(size):
        l.append(randint(lower_bound, upper_bound))
    
    return l 

def zipfian(size):
    return numpy.random.zipf(2, size)

# Assumes that arr is sorted
def histogram(arr, bins):
    hist = [0] * bins
    diff = arr[-1] - arr[0]
    interval = diff // bins

    for i in range(len(arr)):
        index = min((arr[i] - arr[0]) // interval, len(hist) - 1)
        hist[index] += 1

    for i in range(len(hist)):
        hist[i] = hist[i] / len(arr)
    
    return hist

# Need to basically get 900 samples (300 - 16384, 300 - 16384*4, 300 - 16384*8)
# Plan, generate this (will seperate by python and C++ code)
# 1) Generate the following data sets and write them to disk:
#   a) 100 - Normal, 100 - Sequential, 100 - Random: Buffer Size 16384 KV (*2 for int)
#   b) 100 - Normal, 100 - Sequential, 100 - Random: Buffer Size 16384 * 4 KV (*2 for int)
#   c) 100 - Normal, 100 - Sequential, 100 - Random: Buffer Size 16384 * 8 KV (*2 for int)
def writeData(length, func, data_path):
    data = func(length)
    data.sort()

    output_file = open(data_path, 'wb')
    for num in data:
        output_file.write(struct.pack('<i', int(num)))
    output_file.close()

    return None

def raw_data():
    # Naming convention: distrbution_fileNumber_distributionSize.dat
    path_prefix = "raw_data/"
    buffer_sizes = [16384*2, 16384*8, 16384*16]
    func_dic = {"normal": normal, "sequential": sequential, "random": random}
    raw_count = 0

    for buffer_size in buffer_sizes:
        for dist in func_dic:
            dist_type = dist
            true_func = func_dic[dist]

            for i in range(1):
                new_path = path_prefix + dist_type + "_" + str(raw_count) + "_" + str(buffer_size) + ".dat"
                writeData(buffer_size, true_func, new_path)

if __name__ == "__main__":
    raw_data()
