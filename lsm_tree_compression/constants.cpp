// Need to get all the constants for the following variables:
// file_io_to_disk: I/O rate for disk to memory (using ifstream/ofstream -- to keep everything standardized for the system)
// file_io_from_disk: I/O rate for memory to disk (ofstream)
// c_r: hashmap of all the compression rates (kb/second)
// d_r: hashmap of all the decompression rates (kb/second)
// B: DEFAULT_BUFFER_SIZE
#include <unordered_map>
#include <string> 
#include <stdexcept>
#include "lsm_tree.h"
#include <stdlib.h>
#include <vector>
#include "snappy.h"
#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"
#include "zlib.h"
#include "lsm_tree.h"
#include "zstd.h"
#include "RandomForests/RandomForest.h"
#include "constants.h"

using namespace std;

string new_path = "trial.dat";

vector<kv> generate_data() {
    vector<kv> data;

    for(int i = 0; i < DEFAULT_BUFFER_SIZE; i++) {
        int number = rand() % INT_MAX + 1;
        data.push_back({number, number+1});
    }

    return data;
}

float get_file_io_to_disk() {
    vector<kv> data = generate_data();

    clock_t start;
    float duration;
    start = clock();

    // Do all the file operations
    ofstream data_file(new_path, ios::binary);
    data_file.write((const char*)data.data(), data.size());
    data_file.close();

    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    float rate = (float) data.size() * sizeof(kv) / (float) duration;

    return rate;
}

float get_file_io_from_disk() {
    void* buf = malloc(DEFAULT_BUFFER_SIZE * sizeof(kv));
    clock_t start;
    float duration;
    start = clock();

    // Do all the file operations
    ifstream data_file(new_path, ios::ate | ios::binary);
    data_file.read((char*)buf, DEFAULT_BUFFER_SIZE * sizeof(kv));
    data_file.close();

    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    float rate = (float) DEFAULT_BUFFER_SIZE * sizeof(kv) / (float) duration;

    return rate;
}

unordered_map<string, float> get_compression_rates() {
    unordered_map<string, float> rates;
    clock_t start;
    float duration;

    vector<kv> kvs = generate_data();

    // ZStandard Space Savings (5)
    int data_size = sizeof(kv) * kvs.size();
    size_t compressed_buf_size = ZSTD_compressBound(data_size);
    void* compressed_data_zstandard = malloc(compressed_buf_size);

    start = clock();
    // Compress the data: Can change last variable for compression level 1(lowest) to 22(highest)
    size_t zstandard_size = ZSTD_compress(compressed_data_zstandard, compressed_buf_size, (const void*) kvs.data(), data_size, 10);
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    //Insert to rates map
    rates["zstandard_compression_rate"] = (float) kvs.size() * sizeof(kv) / (float) duration;

    // Zstandard Decompress Rate
    void* uncompressed_data_zstandard = (void*) malloc(data_size);

    // Decompress the data
    start = clock();
    size_t new_size = ZSTD_decompress(uncompressed_data_zstandard, data_size, compressed_data_zstandard, zstandard_size);
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to rates map
    rates["zstandard_decompression_rate"] = (float) zstandard_size / (float) duration;
    free(compressed_data_zstandard);
    free(uncompressed_data_zstandard);

    // ZLib Space Savings (4)
    // Get size of the data
    //int data_size = sizeof(kv) * kvs.size();
    uInt output_size = (1.1 * data_size) + 12;
    char* compressed_data = (char*) malloc(output_size);

    // Define the structure for ZLIB
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    defstream.avail_in = (uInt)data_size+1; // size of input, string + terminator
    defstream.next_in = (Bytef *)kvs.data(); // input char array
    defstream.avail_out = (uInt)output_size+1; // size of output
    defstream.next_out = (Bytef *)compressed_data; // output char array
    
    start = clock();
    // the actual compression work.
    deflateInit(&defstream, Z_BEST_SPEED);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    duration = (clock() - start) / (float) CLOCKS_PER_SEC;
    
    size_t zlib_size = defstream.total_out;

    // Insert to rates map
    rates["zlib_compression_rate"] = (float) kvs.size() * sizeof(kv) / (float) duration;

    // Decompress using ZLIB
    // Output buffer of compressed data
    char* uncompressed_data = (char*) malloc(data_size);

    // Define structure for ZLib compression
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    infstream.avail_in = (uInt)zlib_size; // size of input
    infstream.next_in = (Bytef *)compressed_data; // input char array
    infstream.avail_out = (uInt)data_size; // size of output
    infstream.next_out = (Bytef *)uncompressed_data; // output char array

    start = clock();
    // ZLib compression at work
    inflateInit(&infstream);
    inflate(&infstream, Z_NO_FLUSH);
    inflateEnd(&infstream);
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to rates map
    rates["zstandard_decompression_rate"] = (float) zlib_size / (float) duration;
    free(compressed_data);
    free(uncompressed_data);

    // RLE Space Savings (3)
    // Get the initial to make RLE
    int first_value = kvs.at(0).key;
    int diff = kvs.at(0).value - kvs.at(0).key;
    int prev_value = kvs.at(0).value;
    bool can_rle = true;
    size_t rle_size;

    start = clock();
    for(int i = 1; i < kvs.size(); i++) {
        // First check of current key against previous value
        if(diff != kvs.at(i).key - prev_value){
            can_rle = false;
            break;
        }
        // Second check of current value against current key
        if(diff != kvs.at(i).value - kvs.at(i).key){
            can_rle = false;
            break;
        }
        // Update prev_value 
        prev_value = kvs.at(i).value;
    }

    vector<int> to_disk;
    if(can_rle) {
        to_disk.push_back(first_value);
        to_disk.push_back(diff);

        rle_size = to_disk.size() * sizeof(int);
    } 
    else {
        rle_size = DEFAULT_BUFFER_SIZE * sizeof(kv);
    }
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to rates map
    rates["rle_compression_rate"] = (float) kvs.size() * sizeof(kv) / (float) duration;

    start = clock();
    if(can_rle) {
        kv* data = (kv*) malloc(DEFAULT_BUFFER_SIZE * sizeof(kv));
        int* values = (int*) to_disk.data();
        int first_val = values[0];
        int diff = values[1];
        // cout << "In RLE DECODE FOR MERGE: " << filepath << endl;
        // cout << first_val << " " << diff << endl;
        for(int i = 0; i < DEFAULT_BUFFER_SIZE; i++){
            int key = first_val;
            int value = first_val + diff;

            kv curr_kv = {key, value};
            data[i] = curr_kv;

            first_val = value + diff;
        }
        
        free(data);
    }
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to rates map
    rates["rle_decompression_rate"] = (float) kvs.size() * sizeof(kv) / (float) duration;

    // SIMD Size (2)
    // Begin SIMD compression
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("s4-fastpfor-d1");

    vector<uint32_t> compressed_output(kvs.size()*2 + 1024);
    // N+1024 should be plenty

    start = clock();
    size_t compressedsize = compressed_output.size();
    codec.encodeArray((uint32_t*)kvs.data(), kvs.size()*2, compressed_output.data(), compressedsize);
    size_t simd_size = compressedsize * sizeof(uint32_t);

    compressed_output.resize(compressedsize);
    compressed_output.shrink_to_fit();
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to map rates 
    rates["simd_compression_rate"] = (float) kvs.size() * sizeof(kv) / (float) duration;


    uint32_t* mydataback = (uint32_t*) malloc(DEFAULT_BUFFER_SIZE * 2 * sizeof(uint32_t));
    size_t recoveredsize = DEFAULT_BUFFER_SIZE * 2;
    
    start = clock();
    codec.decodeArray((uint32_t*) compressed_output.data(), simd_size/sizeof(uint32_t), mydataback, recoveredsize);
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to map
    rates["simd_decompression_rate"] = (float) simd_size / (float) duration;

    
    // Snappy Size (1)
    string compressed_str;
    start = clock();
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_str);
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;
    size_t snappy_size = compressed_str.size();

    //Insert to rates
    rates["snappy_compression_rate"] = (float) kvs.size() * sizeof(kv) / (float) duration;

    // Decompress Snappy
    string uncompressed_data_snappy;
    start = clock();
    snappy::Uncompress(compressed_str.c_str(), snappy_size, &uncompressed_data_snappy);
    duration = (clock() - start) / (float) CLOCKS_PER_SEC;

    // Insert to map
    rates["snappy_decompression_rate"] = (float) snappy_size / (float) duration;

    return rates;
}

unordered_map<string, float> write_constants_to_file() {
    float file_io_to_disk = get_file_io_to_disk();
    float file_io_from_disk = get_file_io_from_disk();
    unordered_map<string, float> compression_rates = get_compression_rates();
    unordered_map<string, float> full_dic;

    //Iterate through the dictionaries and add them to the full dic
    full_dic.insert(compression_rates.begin(), compression_rates.end());
    full_dic["file_io_to_disk"] = file_io_to_disk;
    full_dic["file_io_from_disk"] = file_io_from_disk;

    return full_dic;
}

void readDataCsv(float** trainset, float* trainlabels, string path) {
    // Create an input filestream
    std::ifstream myFile(path);
	cout << "Path: " << path << endl;
    // Make sure the file is open
    if(!myFile.is_open()) throw std::runtime_error("Could not open file");

	vector<float> features;
	vector<float> labels;

	string line, word, temp;
	int row = 0;

	while(getline(myFile, line)) { 
        features.clear(); 
  
        // read an entire row and 
        // store it in a string variable 'line'   
        // used for breaking words 
        stringstream s(line); 

        // read every column data of a row and 
        // store it in a string variable, 'word' 
        while (getline(s, word, ',')) { 
            // add all the column data 
            // of a row to a vector 
			float data = stof(word);
            features.push_back(data); 
        } 

		labels.push_back(features.back());
		features.pop_back();

		for(int i = 0; i < features.size(); i++) {
			trainset[row][i] = features.at(i);
		}
		row++;
    } 

	for(int i = 0; i < labels.size(); i++) {
		trainlabels[i] = labels.at(i);
	}

}

dataset get_training_data(string path, int TRAIN_NUM, int FEATURE) {
    float** trainset;
	float* trainlabels;

	trainset=new float*[TRAIN_NUM];
	trainlabels=new float[TRAIN_NUM];

	for(int i=0;i<TRAIN_NUM;++i)
	{
		trainset[i]=new float[FEATURE];
	}

    readDataCsv(trainset, trainlabels, path);

    RandomForest* randomForest = new RandomForest(15,20,20,0);
    randomForest->train(trainset,trainlabels,TRAIN_NUM,FEATURE,1,true,1);

    dataset new_dataset = {trainset, trainlabels, randomForest};
    return new_dataset;
}
