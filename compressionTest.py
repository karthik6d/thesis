from random import randint
import string
import subprocess
import csv
import numpy
import struct
import sys

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


def readShellScript():
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

def writeShellScript():
    bash_command = "./writeTest"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
    string_parse = output.decode('utf-8').split('\n')
    uncompressed_size = int(string_parse[0].split(':')[1])
    compressed_size = int(string_parse[3].split(':')[1])
    uncompressed_write = float(string_parse[1].split(':')[1])
    compressed_write = float(string_parse[4].split(':')[1])
    compress_time = float(string_parse[2].split(':')[1])
    total_compression = float(string_parse[5].split(':')[1])
    ratio = float(string_parse[6].split(':')[1])

    #print(output, error)
    dic = {"uncompressed_size": uncompressed_size, 
            "compressed_size": compressed_size, 
            "uncompressed_write": uncompressed_write, 
            "compressed_write": compressed_write,
            "compress_time": compress_time,
            "total_compression": total_compression,
            "ratio": ratio}
    print(dic)
    return dic

def readRunner(data_distribution_func, output_file_name):
    MAX_BUFFER_SIZE = 4096 * 64
    output = []
    default_size = 4096
    increment = 125000000
    target = MAX_BUFFER_SIZE
    counter = 0

    while(default_size <= target):
        print("Iteration: ", counter)
        try:
            writeData(default_size, data_distribution_func)
            output.append(readShellScript())
        except IndexError:
            pass
        counter += 1
        #default_size += increment
        default_size *= 2

    keys = output[0].keys()
    
    with open(output_file_name, 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(output)

    return None

def writeRunner(data_distribution_func, output_file_name):
    output = []
    default_size = 250000
    increment = 125000
    target = 250000 * 20
    counter = 0

    while(default_size <= target):
        print("Iteration: ", counter)
        try:
            writeData(default_size, data_distribution_func)
            output.append(writeShellScript())
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

def main(argv):
    if argv == 'read':
        readRunner(uniform, 'data/uniform_read_1.csv')
        readRunner(sequential, 'data/sequential_read_1.csv')
        readRunner(normal, 'data/normal_read_1.csv')
        readRunner(random, 'data/random_read_1.csv')
        readRunner(zipfian, 'data/zipfian_read_1.csv')
    else:
        writeRunner(uniform, 'data/uniform_write_small.csv')
        writeRunner(sequential, 'data/sequential_write_small.csv')
        writeRunner(normal, 'data/normal_write_small.csv')
        writeRunner(random, 'data/random_write_small.csv')
        writeRunner(zipfian, 'data/zipfian_write_small.csv')

    return None

# size = 10000
# writeData(size, normal)

if __name__ == "__main__":
	 main(sys.argv[1])
