#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include "compression.h"
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <math.h>
#include "snappy.h"
#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"
#include "zlib.h"
#include "lsm_tree.h"
#include "zstd.h"
#include "basic.h"

using namespace std;

int compressed_file_count = 0;

// float optimal_r(unordered_map<string, float> constants, int read_only, int write_only, float leniency, string compression_scheme) {
//     float r = 1.0;
//     if(read_only) {
//         string key = compression_scheme + "_decompression_rate";
//         float numerator = (1.0 + leniency) * constants[key];
//         float denominator = (constants[key] + constants["file_io_from_disk"]) + 0.001;
//         r = numerator / denominator;
//     }
//     else if(write_only) {
//         string key = compression_scheme + "_compression_rate";
//         float first_fraction = (1.0 + leniency);
//         float second_fraction = (constants["file_io_to_disk"] / (constants[key] + 0.001));
//         r = first_fraction - second_fraction;
//     }
//     return r;
// }

float optimal_r(unordered_map<string, float> constants, int read_amount, int write_amount, float leniency, string compression_scheme) {
    float r = 1.0;
    string key_decompression = compression_scheme + "_decompression_rate";
    string key_compression = compression_scheme + "_compression_rate";
    
    // Define all constants
    float A = read_amount * (0.01) * ((N*1.0)/DEFAULT_BUFFER_SIZE);
    float log_value = log(((N*1.0)/DEFAULT_BUFFER_SIZE)) / log(COMPONENTS_PER_LEVEL*1.0);
    float first_num_fraction = (leniency * A * read_amount) / constants["file_io_from_disk"];
    float second_num_fraction = (leniency * write_amount * log_value) / (constants["file_io_to_disk"] * N);
    float third_num_fraction = (write_amount * log_value) / (constants[key_compression] * N);
    float first_den_fraction = (A * read_amount) / constants["file_io_from_disk"];
    float second_den_fraction = (A * read_amount) / constants[key_decompression];
    float third_den_fraction = (write_amount * log_value) / (constants["file_io_to_disk"] * N);

    float numerator = first_num_fraction + second_num_fraction + third_num_fraction;
    float denominator = first_den_fraction + second_den_fraction + third_den_fraction;

    r = numerator / denominator;
    return r;
}

float* histogram(vector<kv> arr, int bins, int min_val, int max_val) {
    float* hist = (float*) malloc(sizeof(float) * bins);
    long long int diff = max_val - min_val;
    int interval = diff / bins;

    for(int i = 0; i < bins; i++) {
        hist[i] = 0.0;
    }

    for(int i = 0; i < arr.size(); i++) {
        int diff1 = arr.at(i).key - min_val;
        int index_key = min(diff1 / interval, bins - 1);
        hist[index_key] += 1.0;

        int diff2 = arr.at(i).value - min_val;
        int index_value = min(diff2 / interval, bins - 1);
        hist[index_value] += 1.0;
    }

    for(int i = 0; i < bins; i++) {
        hist[i] = hist[i] / (arr.size() * 2);
    }

    return hist;
}

int best_scheme(vector<kv> kvs, int min_val, int max_val) {
    //cout << "Has to be here" << endl;
    // Print out what the R's should be to hold this leniency model
    vector<string> compression_schemes;
    compression_schemes.push_back("snappy");
    compression_schemes.push_back("simd");
    compression_schemes.push_back("rle");
    compression_schemes.push_back("zlib");
    compression_schemes.push_back("zstandard");

    unordered_map<string, float> optimal_rs;
    unordered_map<string, float> predicted_rs;

    // for(auto it : current_db->constants) {
    //     cout << it.first << ":" << it.second << endl;
    // }

    for(int i = 0; i < compression_schemes.size(); i++) {
        optimal_rs[compression_schemes.at(i)] = optimal_r(current_db -> constants, current_db->read_amount, current_db->write_amount, current_db->leniency, compression_schemes.at(i));
    }

    vector<kv> kvs_copy(kvs);
    //cout << "Getting the best compression rates" << endl;
    predicted_rs = getCompressionRates(kvs_copy);
    //cout << "After that" << endl;
    // // std::sort(kvs.begin(), kvs.end(), compare_kvs);
    // float* hist = histogram(kvs, 50, min_val, max_val);

    // for(int i = 0; i < 50; i++) {
    //     cout << hist[i] << ",";
    // }
    // cout << endl;
    // cout << "is something wrong with creating the histogram";
    // // Do the prediction here
    int best_compression = 0;
    float best_compression_ratio = 1.0;

    for(int i = 0; i < compression_schemes.size(); i++) {
    //   float predicted_r;
    //   current_db->models[compression_schemes.at(i)].rf->predict(hist, predicted_r);
    //   float max_optimal_r = optimal_rs[compression_schemes.at(i)];
    //   float diff = abs(predicted_r - max_optimal_r);
    //   cout << compression_schemes.at(i) << ": Predicted R: " << predicted_r << " Max_Optimal_R: " << max_optimal_r << endl;
        float predicted_r = predicted_rs[compression_schemes.at(i)];
        float optimal_r = optimal_rs[compression_schemes.at(i)];
        float diff = abs(predicted_r - optimal_r);

        //cout << compression_schemes.at(i) << ": Predicted R: " << predicted_r << " Max_Optimal_R: " << optimal_r << endl;

        if(predicted_r < optimal_r) {
            if(predicted_r < best_compression_ratio) {
                best_compression = i+1;
                best_compression_ratio = predicted_r;
            }
        }
    }

    // cout << "Best Compression: " << best_compression << endl;
    // cout << "Optimal Compression Ratio: " << best_compression_ratio << endl;

    return best_compression;
}

string ZSTANDARD_encode(vector<kv> kvs) {
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);

    // Get size of compressed data using ZStandard library
    int data_size = sizeof(kv) * kvs.size();
    size_t compressed_buf_size = ZSTD_compressBound(data_size);
    void* compressed_data = malloc(compressed_buf_size);

    // Compress the data: Can change last variable for compression level 1(lowest) to 22(highest)
    size_t actual_size = ZSTD_compress(compressed_data, compressed_buf_size, (const void*) kvs.data(), data_size, 10);

    // Write data to disk
    data_file.write((const char*)compressed_data, actual_size);
    data_file.close();

    free(compressed_data);
    return new_path;
}

kv* ZSTANDARD_decode(string filepath) {
    ifstream f(filepath, ios::ate | ios::binary);
    int length = f.tellg();

    f.seekg(0, f.beg);

    //Prepare the buffer with compressed data from disk
    char* buf = (char*) malloc(length);
    f.read((char*)buf, length);

    // Create the output data
    int data_size = DEFAULT_BUFFER_SIZE * sizeof(kv);
    void* uncompressed_data = (void*) malloc(data_size);

    // Decompress the data
    size_t new_size = ZSTD_decompress(uncompressed_data, data_size, (void*) buf, length);

    free(buf);
    f.close();
    return (kv*)uncompressed_data;
}

string ZLIB_encode(vector<kv> kvs) {
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);

    // Get size of the data
    int data_size = sizeof(kv) * kvs.size();
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
    
    // the actual compression work.
    deflateInit(&defstream, Z_BEST_SPEED);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    // Write the data to disk
    data_file.write((const char*)compressed_data, defstream.total_out);
    data_file.close();

    free(compressed_data);
    return new_path;
}

kv* ZLIB_decode(string filepath) {
    ifstream f(filepath, ios::ate | ios::binary);
    int length = f.tellg();

    f.seekg(0, f.beg);

    //Prepare the buffer with compressed data from disk
    char* buf = (char*) malloc(length);
    f.read((char*)buf, length);

    // Output buffer of compressed data
    int data_size = DEFAULT_BUFFER_SIZE * sizeof(kv);
    char* uncompressed_data = (char*) malloc(data_size);

    // Define structure for ZLib compression
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    infstream.avail_in = (uInt)length; // size of input
    infstream.next_in = (Bytef *)buf; // input char array
    infstream.avail_out = (uInt)data_size; // size of output
    infstream.next_out = (Bytef *)uncompressed_data; // output char array

    // ZLib compression at work
    inflateInit(&infstream);
    inflate(&infstream, Z_NO_FLUSH);
    inflateEnd(&infstream);

    free(buf);
    f.close();
    return (kv*) uncompressed_data;
}

string RLE_encode(vector<kv> kvs) {
    // Create the output file
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);
    
    // Get the initial to make RLE
    int first_value = kvs.at(0).key;
    int diff = kvs.at(0).value - kvs.at(0).key;
    int prev_value = kvs.at(0).value;
    bool can_rle = true;

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

    if(can_rle) {
        vector<int> to_disk;
        to_disk.push_back(first_value);
        to_disk.push_back(diff);

        data_file.write((const char*)to_disk.data(), to_disk.size()*sizeof(int));
        data_file.close();

        return new_path;
    }

    data_file.write((const char*)kvs.data(), kvs.size()*sizeof(kv));
    data_file.close();

    return new_path;
}

kv* RLE_decode(string filepath) {
    ifstream f(filepath, ios::ate | ios::binary);
    int length = f.tellg();

    f.seekg(0, f.beg);

    //Prepare the buffer
    char* buf = (char*) malloc(length);
    f.read((char*)buf, length);

    // Decode
    if(length != DEFAULT_BUFFER_SIZE * sizeof(kv)){
        kv* data = (kv*) malloc(DEFAULT_BUFFER_SIZE * sizeof(kv));
        int* values = (int*) buf;
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
        
        free(buf);
        f.close();
        return data;
    }

    f.close();
    return (kv*) buf;
}

string SIMD_encode(std::vector<kv> kvs){
    // Create the output file
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);

    // Begin SIMD compression
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("s4-fastpfor-d1");

    vector<uint32_t> compressed_output(kvs.size()*2 + 1024);
    // N+1024 should be plenty

    size_t compressedsize = compressed_output.size();
    codec.encodeArray((uint32_t*)kvs.data(), kvs.size()*2, compressed_output.data(), compressedsize);

    // if desired, shrink back the array:
    compressed_output.resize(compressedsize);
    compressed_output.shrink_to_fit();

    data_file.write((const char*)compressed_output.data(), compressedsize*sizeof(uint32_t));
    data_file.close();

    return new_path;
}

kv* SIMD_decode(string filepath) {
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("s4-fastpfor-d1");
    ifstream f(filepath, ios::ate | ios::binary);
    int length = f.tellg();

    f.seekg(0, f.beg);

    //Prepare the buffer
    char* buf = (char*) malloc(length);
    f.read((char*)buf, length);

    //cout << length << endl;

    uint32_t* mydataback = (uint32_t*) malloc(DEFAULT_BUFFER_SIZE * 2 * sizeof(uint32_t));
    size_t recoveredsize = DEFAULT_BUFFER_SIZE * 2;
    
    codec.decodeArray((uint32_t*) buf, length/sizeof(uint32_t), mydataback, recoveredsize);
    //cout << "Recovered Size: " << endl;
    
    free(buf);
    f.close();
    return (kv*) mydataback;
}

int compare_these_kvs(kv a, kv b) {
  int a_val = a.key < 0 ? -a.key : a.key;
  int b_val = b.key < 0 ? -b.key : b.key;
  return a_val < b_val;
}

string SNAPPY_encode(vector<kv> kvs) {
    // Create the output file
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);

    //cout << "FILE PATH: " << new_path << endl;
    string compressed_str;
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_str);
    size_t snappy_size = compressed_str.size();
    cout << "Uncompressed Size: " << kvs.size()*sizeof(kv) << " Compressed Size: " << snappy_size << endl; 
    data_file.write(compressed_str.c_str(), compressed_str.size());
    data_file.close();

    return new_path;
}

kv* SNAPPY_decode(string filepath) {
    ifstream f(filepath, ios::ate | ios::binary);
    int length = f.tellg();

    f.seekg(0, f.beg);

    //Prepare the buffer
    char* buf = (char*) malloc(length);
    f.read((char*)buf, length);

    string uncompressed_data;
    snappy::Uncompress(buf, length, &uncompressed_data);

    free(buf);
    f.close();
    return (kv*) uncompressed_data.c_str();
}

string new_compressed_file() {
    string new_path = "enc/";
    new_path += to_string(compressed_file_count++);
    new_path += ".dat";

    return new_path;
}
