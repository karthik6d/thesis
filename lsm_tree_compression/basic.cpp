#include<vector>
#include<string>
#include <unordered_map>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include "snappy.h"
#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"
#include "zlib.h"
#include "zstd.h"
#include "lsm_tree.h"

using namespace std;


size_t zstandard_space(vector<kv> kvs){
    // ZStandard Space Savings (5)
    int data_size = sizeof(kv) * kvs.size();
    size_t compressed_buf_size = ZSTD_compressBound(data_size);
    void* compressed_data_zstandard = malloc(compressed_buf_size);

    // Compress the data: Can change last variable for compression level 1(lowest) to 22(highest)
    size_t zstandard_size = ZSTD_compress(compressed_data_zstandard, compressed_buf_size, (const void*) kvs.data(), data_size, 10);

    return zstandard_size;
}

size_t zlib_space(vector<kv> kvs) {
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
    
    //cout << "Where the seg fault at" << endl;
    // the actual compression work.
    deflateInit(&defstream, Z_BEST_SPEED);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    //cout << "Why isn't this compression working" << endl;

    size_t zlib_size = defstream.total_out;
    return zlib_size;
}

size_t rle_space(vector<kv> kvs) {
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

    return rle_size;
}

size_t simd_space(vector<kv> kvs) {
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("s4-fastpfor-d1");

    vector<uint32_t> compressed_output(kvs.size()*2 + 1024);
    // N+1024 should be plenty

    size_t compressedsize = compressed_output.size();
    codec.encodeArray((uint32_t*)kvs.data(), kvs.size()*2, compressed_output.data(), compressedsize);
    size_t simd_size = compressedsize * sizeof(uint32_t);

    return simd_size;
}

size_t snappy_space(vector<kv> kvs) {
    // Snappy Size (1)
    string compressed_str;
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_str);
    size_t snappy_size = compressed_str.size();

    return snappy_size;
}

unordered_map<string, float> getCompressionRates(vector<kv> kvs) {
    int best_size = DEFAULT_BUFFER_SIZE;
    vector<kv> kvs_zstandard(kvs);
    vector<kv> kvs_zlib(kvs);
    vector<kv> kvs_rle(kvs);
    vector<kv> kvs_simd(kvs);
    vector<kv> kvs_snappy(kvs);

    // Compress the data: Can change last variable for compression level 1(lowest) to 22(highest)
    size_t zstandard_size = zstandard_space(kvs_zstandard);

    size_t zlib_size = DEFAULT_BUFFER_SIZE * sizeof(kv) * 2;

    size_t rle_size = rle_space(kvs_rle);

    size_t simd_size = simd_space(kvs_simd);

    size_t snappy_size = snappy_space(kvs_snappy);

    //cout << "SEG FAULT5" << endl;
    // Figure out the best one based on size
    // cout << "DEFAULT BUFFER SIZE: " << (DEFAULT_BUFFER_SIZE * sizeof(kv)) << endl;
    // cout << "Snappy: " << (float)snappy_size / (float) (DEFAULT_BUFFER_SIZE * sizeof(kv))<< endl;
    // cout << "Zlib: " << zlib_size << endl;
    // cout << "Zstandard: " << zstandard_size << endl;
    unordered_map<string, float> compression_rates;
    compression_rates["snappy"] = (float) snappy_size / (float) (DEFAULT_BUFFER_SIZE * sizeof(kv));
    compression_rates["simd"] = (float) simd_size / (float) (DEFAULT_BUFFER_SIZE * sizeof(kv));
    compression_rates["rle"] = (float) rle_size / (float) (DEFAULT_BUFFER_SIZE * sizeof(kv));
    compression_rates["zlib"] = (float) zlib_size / (float) (DEFAULT_BUFFER_SIZE * sizeof(kv));
    compression_rates["zstandard"] = (float) zstandard_size / (float) (DEFAULT_BUFFER_SIZE * sizeof(kv));

    return compression_rates;
}

