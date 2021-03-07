import subprocess
import os
from os import listdir
from os.path import isfile, join
import csv

def createData(fileName, bufferSize, keyDistribution, valueDistribution):
    os.chdir('../lsm_tree_compression')
    bash_command = """./gen -N {bufferSize} -keyDistribution {keyDistribution} -valueDistribution {valueDistribution} -dataFile {fileName}""".format(bufferSize=bufferSize, keyDistribution=keyDistribution, valueDistribution=valueDistribution, fileName=fileName)
    #print(bash_command)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return None

def generateDataFiles():
    keyDistributions = {"uniformKey": 0, "sequentialKey": 1, "normalKey":2}
    valueDistributions = {"uniformValue": 0, "sameValue": 1}

    buffer_sizes = [16384, 16384*4, 16384*8]
    NUM_ENTRIES = 1
    path_prefix = "../data_sketching/raw_data/"
    raw_count = 0


    for buffer_size in buffer_sizes:
        for keydist in keyDistributions:
            for valuedist in valueDistributions:
                for _ in range(NUM_ENTRIES):
                    new_path = path_prefix + keydist + "_" + valuedist + "_" + str(raw_count) + "_" + str(buffer_size) + ".csv"
                    createData(new_path, buffer_size, keyDistributions[keydist], valueDistributions[valuedist])
                    raw_count += 1
    
    return None

def datasetCreation(): 
    full_data = {"snappy": [], "simd": [], "rle": [], "zlib": [], "zstandard": []}
    mypath = "raw_data"

    # Get all files form raw_data directors
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

    # Run the data generation process for each file in the list
    os.chdir("data_processing")
    for file in onlyfiles:
        curr_path = "../" + mypath + "/" + file
        print(curr_path)
        bash_command = "./basic {path}".format(path = curr_path)
        process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        if not error:
            # Retrieve the data from the created json files
            # Get the histogram data first
            hist_path = "../clean_data/hist.csv"
            hist = []
            with open(hist_path, newline='') as csvfile:
                reader = csv.reader(csvfile)
                hist = list(reader)
            
            # Clean up hist file
            hist = hist[0]
            hist.pop()
            hist = list(map(float, hist))

            # Retrieve the data from the compression ratios file
            compression_path = "../clean_data/compression_ratios.csv"
            compression_ratios = {}
            input_file = csv.reader(open(compression_path))

            for row in input_file:
                k,v = row
                compression_ratios[k] = float(v)
            
            # Add all the data to the large data file
            for scheme in compression_ratios:
                hist_copy = hist.copy()
                hist_copy.append(compression_ratios[scheme])
                full_data[scheme].append(hist_copy)
    
    #print(full_data)
    for scheme in full_data:
        path = "../clean_data/" + scheme + ".csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(full_data[scheme])
    
    return None
        
if __name__ == "__main__":
    datasetCreation()
