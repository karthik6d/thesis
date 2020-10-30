import random
import string
import subprocess
import csv

def createData(length):
    letters = string.ascii_lowercase + ' '
    input_str = ''.join('1' for i in range(length))

    # input_str = 'This is a snappy example.'
    # for _ in range(10):
    #     input_str += input_str
    
    filehandle = open('data/sample.txt', 'w')
    filehandle.write(input_str)
    filehandle.close()

    return None

def shellScript():
    bash_command = "./test {query_path}".format(query_path='data/sample.txt')
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, _ = process.communicate()
    
    string_parse = output.decode('utf-8').split('\n')
    uncompressed_size = int(string_parse[0].split(':')[1])
    uncompressed_read = float(string_parse[2].split(':')[1])
    compressed_size = int(string_parse[5].split(':')[1])
    compressed_read = float(string_parse[-3].split(':')[1])

    # print("Uncompressed Size: ", uncompressed_size)
    # print("Uncompressed Read: ", uncompressed_read)
    # print("Compressed Size: ", compressed_size)
    # print("Compressed Read: ", compressed_read)

    return {"uncompressed_size": uncompressed_size, "uncompressed_read": uncompressed_read, "compressed_size": compressed_size, "compressed_read": compressed_read}

def main():
    output = []
    default_size = 1000000

    for i in range(1,150):
        print("Iteration: ", i)
        size = i * default_size
        createData(size)
        output.append(shellScript())

    keys = output[0].keys()
    
    with open('data/compression_data.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(output)

    return None

if __name__ == "__main__":
    main()