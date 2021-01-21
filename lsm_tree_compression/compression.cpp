#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "compression.h"
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include "snappy.h"
#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"
#include "lsm_tree.h"

using namespace std;

int compressed_file_count = 0;

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
        return data;
    }

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

    vector<uint32_t> mydataback(kvs.size() * 2);
    size_t recoveredsize = mydataback.size();
    
    codec.decodeArray(compressed_output.data(), compressed_output.size(), mydataback.data(), recoveredsize);
    mydataback.resize(recoveredsize);

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
    
    free(buf);
    return (kv*) mydataback;
}

string SNAPPY_encode(vector<kv> kvs) {
    // Create the output file
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);

    string compressed_data;
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_data);

    data_file.write(compressed_data.c_str(), compressed_data.size());
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
    return (kv*) uncompressed_data.c_str();
}

string new_compressed_file() {
    string new_path = "enc/";
    new_path += to_string(compressed_file_count++);
    new_path += ".dat";

    return new_path;
}
