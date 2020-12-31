import os
import subprocess
import time
import csv

distributions = {0: "uniformKey", 1: "sequentialKey", 2: "normalKey"}
compression_schemes = {0: "kNoCompression", 1: "kSnappyCompression", 2: 'kZlibCompression', 3: "kBZip2Compression", 
                        4: "kLZ4Compression", 5: "kLZ4HCCompression", 6: "kXpressCompression", 7: "kZSTD"}
# NUM_KV_PAIRS = 10000000
# NUM_QUERIES = 100000
# NUM_TRIALS = 5

NUM_KV_PAIRS = 1000
NUM_QUERIES = 10
NUM_TRIALS = 2

# Runs the bottommost compression trial on RocksDB
def runBottommostTrial(compression_scheme):
    os.chdir('../rocksdb')
    bash_command = "./example {compression_scheme}".format(compression_scheme=compression_scheme)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, _ = process.communicate()

    time = float(output.decode('utf-8').split('\n')[-2].split(':')[1])
    return time

# Creates the data from the Go workload generator 
def createData(size, numberQueries, dataDistribution):
    os.chdir('../lsm_tree_compression')
    bash_command = """./gen -N {size} -queries {numberQueries} -keyDistribution {dataDistribution} -readPercentage 70""".format(size=size, numberQueries=numberQueries, dataDistribution=dataDistribution)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()

    return None

# Runs each bottomost trial with RocksDB
def bottomostCompressionTrial(num_trials, compression_scheme):
    avg_time = 0.0

    for _ in range(num_trials):
        avg_time += runBottommostTrial(compression_scheme)

    return avg_time / num_trials

# Main Function for bottommost compression
def main():
    # Step 1: Generate the workload for each data distribution (3 total data distributions)
    # Step 2: Run the bottommostCompressionTrial (5 trials) for each compression scheme (7 total)
    # Step 3: Record each result, store it in a dictionary and save to a CSV
    all_data = []
    num_trial = 0

    for distribution in distributions:
        curr_run = {"KV Pairs": NUM_KV_PAIRS, "Num Reads": NUM_QUERIES * .7, "Num Writes": NUM_QUERIES*.3, 
                    "Data Distribution": distributions[distribution], "Number Trials": NUM_TRIALS}
        createData(NUM_KV_PAIRS, NUM_QUERIES, distribution)
        
        for compression in compression_schemes:
            time = bottomostCompressionTrial(NUM_TRIALS, compression)
            curr_run['Compression Scheme'] = compression_schemes[compression]
            curr_run['Avg. Time'] = time
            all_data.append(curr_run.copy())
            print("Trial: ", num_trial, " Data Distribution: ", distributions[distribution], " Compression Scheme: ", compression_schemes[compression])
            num_trial += 1
    
    # Write the results to a csv file
    with open('bottomost_trial.csv', 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file, fieldnames=all_data[0].keys())
        fc.writeheader()
        fc.writerows(all_data)
    
    return None

if __name__ == '__main__':
    main()





