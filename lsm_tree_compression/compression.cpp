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
#include "snappy.h"
#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"
#include "zlib.h"
#include "lsm_tree.h"
#include "zstd.h"

using namespace std;

int compressed_file_count = 0;

int best_scheme(vector<kv> kvs) {
    int best_size = DEFAULT_BUFFER_SIZE;

    // ZStandard Space Savings (5)
    int data_size = sizeof(kv) * kvs.size();
    size_t compressed_buf_size = ZSTD_compressBound(data_size);
    void* compressed_data_zstandard = malloc(compressed_buf_size);

    // Compress the data: Can change last variable for compression level 1(lowest) to 22(highest)
    size_t zstandard_size = ZSTD_compress(compressed_data_zstandard, compressed_buf_size, (const void*) kvs.data(), data_size, 10);

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
    
    // the actual compression work.
    deflateInit(&defstream, Z_BEST_SPEED);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    size_t zlib_size = defstream.total_out;

    // RLE Space Savings (3)
    // Get the initial to make RLE
    int first_value = kvs.at(0).key;
    int diff = kvs.at(0).value - kvs.at(0).key;
    int prev_value = kvs.at(0).value;
    bool can_rle = true;
    size_t rle_size;

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

        rle_size = to_disk.size() * sizeof(int);
    } 
    else {
        rle_size = DEFAULT_BUFFER_SIZE * sizeof(kv);
    }

    // SIMD Size (2)
    // Begin SIMD compression
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("s4-fastpfor-d1");

    vector<uint32_t> compressed_output(kvs.size()*2 + 1024);
    // N+1024 should be plenty

    size_t compressedsize = compressed_output.size();
    codec.encodeArray((uint32_t*)kvs.data(), kvs.size()*2, compressed_output.data(), compressedsize);
    size_t simd_size = compressedsize;

    // Snappy Size (1)
    string compressed_str;
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_str);
    size_t snappy_size = compressed_str.size();

    // Figure out the best one based on size
    size_t sizes[] = {DEFAULT_BUFFER_SIZE * sizeof(kv), snappy_size, simd_size, rle_size, zlib_size, zstandard_size};
    cout << "Sizes: " << DEFAULT_BUFFER_SIZE * sizeof(kv) << ", " << snappy_size << ", " << simd_size << ", " << rle_size << ", " << zlib_size << ", " << zstandard_size << endl;
    size_t best = DEFAULT_BUFFER_SIZE * sizeof(kv);
    int best_scheme = 0;

    for(int i = 0; i < 6; i ++) {
        if(sizes[i] < best) {
            best = sizes[i];
            best_scheme = i;
        }
    }
    cout << "Best Scheme: " << best_scheme << endl;
    return best_scheme;
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

string SNAPPY_encode(vector<kv> kvs) {
    // Create the output file
    std::string new_path = new_compressed_file();
    ofstream data_file(new_path, ios::binary);

    //cout << "FILE PATH: " << new_path << endl;
    string compressed_data;
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_data);
    //cout << "Uncompressed Size: " << kvs.size()*sizeof(kv) << " Compressed Size: " << compressed_data.size() << endl; 
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
    f.close();
    return (kv*) uncompressed_data.c_str();
}

string new_compressed_file() {
    string new_path = "enc/";
    new_path += to_string(compressed_file_count++);
    new_path += ".dat";

    return new_path;
}
