from random import randint
import string
import subprocess
import csv
import numpy
import struct

def writeData(length, func):
    data = func(length)

    output_file = open('data/sample.dat', 'wb')
    for num in data:
        output_file.write(struct.pack('<i', int(num)))
    output_file.close()

    return None

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

    return l

def random(size):
    lower_bound = 1
    upper_bound = 1000
    l = []

    for i in range(size):
        l.append(randint(lower_bound, upper_bound))
    
    return l 

def zipfian(size):
    return numpy.random.zipf(2, size)


def shellScript():
    bash_command = "./readTest"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
    string_parse = output.decode('utf-8').split('\n')
    uncompressed_size = int(string_parse[0].split(':')[1])
    compressed_size = int(string_parse[1].split(':')[1])
    uncompressed_read = float(string_parse[2].split(':')[1])
    compressed_read = float(string_parse[3].split(':')[1])
    decompress_time = float(string_parse[4].split(':')[1])
    total_decompression = float(string_parse[5].split(':')[1])
    ratio = float(string_parse[6].split(':')[1])

    #print(output, error)
    dic = {"uncompressed_size": uncompressed_size, 
            "compressed_size": compressed_size, 
            "uncompressed_read": uncompressed_read, 
            "compressed_read": compressed_read,
            "decompress_time": decompress_time,
            "total_decompression": total_decompression,
            "ratio": ratio}
    print(dic)
    return dic

def runner(data_distribution_func, output_file_name):
    output = []
    default_size = 25000000
    increment = 12500000
    target = 500000000
    counter = 0

    while(default_size <= target):
        print("Iteration: ", counter)
        try:
            writeData(default_size, data_distribution_func)
            output.append(shellScript())
        except IndexError:
            pass
        counter += 1
        default_size += increment

    keys = output[0].keys()
    
    with open(output_file_name, 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(output)

    return None

def main():
    runner(uniform, 'data/uniform_read.csv')
    runner(sequential, 'data/sequential_read.csv')
    runner(normal, 'data/normal_read.csv')
    runner(random, 'data/random_read.csv')
    runner(zipfian, 'data/zipfian_read.csv')

    return None

if __name__ == "__main__":
    main()
