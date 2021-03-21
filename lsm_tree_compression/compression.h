#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include "snappy.h"
#include "lsm_tree.h"
#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"

extern int compressed_file_count;
extern SIMDCompressionLib::IntegerCODEC &codec;

// Standardize how we create new compression files
string new_compressed_file();

// Functional tool to compute the BEST compression scheme based on space
int best_scheme(std::vector<kv> kvs, int min_val, int max_val);

//Functionality to get the optimal R's
float optimal_r(unordered_map<string, float> constants, bool read_only, bool write_only, float leniency, string compression_scheme);

//Functionality to create a histogram from the data
float* histogram(vector<kv> arr, int bins, int min_val, int max_val);

// Snappy Compression
std::string SNAPPY_encode(std::vector<kv> kvs);
kv* SNAPPY_decode(string filepath);

// SIMD Functionality
std::string SIMD_encode(std::vector<kv> kvs);
kv* SIMD_decode(string filepath);

//Basic RLE for Testing
string RLE_encode(vector<kv> kvs);
kv* RLE_decode(string filepath);

//ZLib Functionality
std::string ZLIB_encode(std::vector<kv> kvs);
kv* ZLIB_decode(string filepath);

//ZStandard Functionality
std::string ZSTANDARD_encode(std::vector<kv> kvs);
kv* ZSTANDARD_decode(string filepath);