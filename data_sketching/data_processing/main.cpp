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

using namespace std;
namespace fs = std::filesystem;

#define BINS 50

typedef struct kv {
  int key;
  int value;
} kv;

int compare_kvs(kv a, kv b) {
  int a_val = a.key < 0 ? -a.key : a.key;
  int b_val = b.key < 0 ? -b.key : b.key;
  return a_val < b_val;
}

size_t zlib_space(vector<kv> kvs, int DEFAULT_BUFFER_SIZE) {
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
    cout << "HUH" << endl;
    deflate(&defstream, Z_FINISH);
    cout << "Here" << endl;
    deflateEnd(&defstream);

    //cout << "Why isn't this compression working" << endl;

    size_t zlib_size = defstream.total_out;
    return zlib_size;
}

unordered_map<string, float> getCompressionRates(vector<kv> kvs, int DEFAULT_BUFFER_SIZE) {
    int best_size = DEFAULT_BUFFER_SIZE;

    // ZStandard Space Savings (5)
    int data_size = sizeof(kv) * kvs.size();
    size_t compressed_buf_size = ZSTD_compressBound(data_size);
    void* compressed_data_zstandard = malloc(compressed_buf_size);

    // Compress the data: Can change last variable for compression level 1(lowest) to 22(highest)
    size_t zstandard_size = ZSTD_compress(compressed_data_zstandard, compressed_buf_size, (const void*) kvs.data(), data_size, 10);

    //cout << "SEG FAULT1" << endl;
    // ZLib Space Savings (4)
    // Get size of the data
    //int data_size = sizeof(kv) * kvs.size();
    // uInt output_size = (1.1 * data_size) + 12;
    // char* compressed_data = (char*) malloc(output_size);

    // // Define the structure for ZLIB
    // z_stream defstream;
    // defstream.zalloc = Z_NULL;
    // defstream.zfree = Z_NULL;
    // defstream.opaque = Z_NULL;
    // defstream.avail_in = (uInt)data_size+1; // size of input, string + terminator
    // defstream.next_in = (Bytef *)kvs.data(); // input char array
    // defstream.avail_out = (uInt)output_size+1; // size of output
    // defstream.next_out = (Bytef *)compressed_data; // output char array
    
    // //cout << "Where the seg fault at" << endl;
    // // the actual compression work.
    // deflateInit(&defstream, Z_BEST_SPEED);
    // cout << "HUH" << endl;
    // deflate(&defstream, Z_FINISH);
    // cout << "Here" << endl;
    // deflateEnd(&defstream);

    //cout << "Why isn't this compression working" << endl;

    size_t zlib_size = zlib_space(kvs, DEFAULT_BUFFER_SIZE);

    //cout << "SEG FAULT2" << endl;

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

    //cout << "SEG FAULT3" << endl;

    // SIMD Size (2)
    // Begin SIMD compression
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("s4-fastpfor-d1");

    vector<uint32_t> compressed_output(kvs.size()*2 + 1024);
    // N+1024 should be plenty

    size_t compressedsize = compressed_output.size();
    codec.encodeArray((uint32_t*)kvs.data(), kvs.size()*2, compressed_output.data(), compressedsize);
    size_t simd_size = compressedsize;

    //cout << "SEG FAULT4" << endl;

    // Snappy Size (1)
    string compressed_str;
    snappy::Compress((char*) kvs.data(), kvs.size()*sizeof(kv), &compressed_str);
    size_t snappy_size = compressed_str.size();

    //cout << "SEG FAULT5" << endl;
    // Figure out the best one based on size
    unordered_map<string, float> compression_rates;
    compression_rates["snappy"] = (float) snappy_size / (float) DEFAULT_BUFFER_SIZE * sizeof(kv);
    compression_rates["simd"] = (float) simd_size / (float) DEFAULT_BUFFER_SIZE * sizeof(kv);
    compression_rates["rle"] = (float) rle_size / (float) DEFAULT_BUFFER_SIZE * sizeof(kv);
    compression_rates["zlib"] = (float) zlib_size / (float) DEFAULT_BUFFER_SIZE * sizeof(kv);
    compression_rates["zstandard"] = (float) zstandard_size / (float) zstandard_size * sizeof(kv);

    return compression_rates;
}

int getBufferSize(string path) {
    string buf;
    istringstream ss(path);

    vector<string> tokens;

    while(getline(ss, buf, '/')) {
        tokens.push_back(buf);
    }

    string buf2;
    istringstream sss(tokens.at(tokens.size()-1));

    vector<string> tokens2; 
    while(getline(sss, buf2, '_')) {
        tokens2.push_back(buf2);
    }

    string buf3;
    istringstream ssss(tokens2.at(tokens2.size() - 1));

    vector<string> tokens3;
    while(getline(ssss, buf3, '.')) {
        tokens3.push_back(buf3);
    }

    return stoi(tokens3.at(0));
}

vector<float> histogram(vector<kv> arr, int bins) {
    float* hist = (float*) malloc(bins * sizeof(int));
    int diff = arr.at(arr.size() - 1).value - arr.at(0).key;
    int interval = diff / bins;

    for(int i = 0; i < arr.size(); i++) {
        int diff1 = arr.at(i).key - arr.at(0).key;
        int index_key = min(diff1 / interval, bins - 1);
        hist[index_key] += 1.0;

        int diff2 = arr.at(i).value - arr.at(0).key;
        int index_value = min(diff2 / interval, bins - 1);
        hist[index_value] += 1.0;
    }

    vector<float> hist_data;
    for(int i = 0; i < bins; i++) {
        hist[i] = hist[i] / (float)(arr.size() * 2);
        hist_data.push_back(hist[i]);
    }

    free(hist);
    return hist_data;
}

vector<kv> load(string path) {
  // Read data from CSV File
  ifstream data_file(path, ios::ate);

  int length = data_file.tellg();
  data_file.close();

  FILE* f = fopen(path.c_str(), "r");

  char* data =
      (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);
  fclose(f);

  string data_str(data, data + length);

  // if(munmap(data, length) != 0){
  //   cout << strerror(errno) << endl;
  // }

  int i = 0;

  int lines = count(data_str.begin(), data_str.end(), '\n');

  istringstream ss(data_str);

  string line;
  vector<kv> kvs;


  while (getline(ss, line)) {
    // cout << "\r" << ++i << "/" << lines << flush;

    istringstream ss(line);

    string key_s, value_s;

    getline(ss, key_s, ',');
    getline(ss, value_s, ',');

    kv pair = {stoi(key_s), stoi(value_s)};
    kvs.push_back(pair);
  }
  std::sort(kvs.begin(), kvs.end(), compare_kvs);
  return kvs;
}


int* dataReader(string folder) {
    unordered_map<string, vector<vector<float> > > full_data 
    { {"snappy", vector<vector<float> > ()},
      {"simd", vector<vector<float> > ()},
      {"rle", vector<vector<float> > ()},
      {"zlib", vector<vector<float> > ()},
      {"zstandard", vector<vector<float> > ()}
    };

    for(const auto &entry : fs::directory_iterator(folder)) {
        cout << entry.path() << endl;
        int buffer_size = getBufferSize(entry.path());
        string new_path = entry.path();
        cout << "PATH: " << new_path << " Buffer Size: " << buffer_size << endl; 
        vector<kv> kvs = load(new_path);
        cout << "Load works" << endl;
        unordered_map<string, float> compression_rates = getCompressionRates(kvs, buffer_size);
        cout << "Compression Rates work" << endl;
        vector<float> hist = histogram(kvs, BINS);
        cout << "Histogram works" << endl;
        vector<float> hist_copy(hist);

        for(auto it : compression_rates) {
            string scheme = it.first;
            float rate = it.second;
            hist_copy.push_back(rate);
            cout << scheme << ":" << rate << endl;
            full_data[scheme].push_back(hist_copy);
        }
        cout << "Did i fuck up the for loop" << endl;
    }

    for(auto it : full_data) {
        std::ofstream myfile;
        string path = "../clean_data/" + it.first + ".csv";
        myfile.open(path);

        vector<vector<float> > curr_data = it.second;
        for(vector<float> row : curr_data) {
            for(float val : row) {
                myfile << val << ",";
            }
            myfile << "\n";
        }
    }

    return NULL;
}

int main() {
    string folder = "../raw_data";
    for(int i = 0; i < 100; i ++) {
        dataReader(folder);
    }

    return 1;
}