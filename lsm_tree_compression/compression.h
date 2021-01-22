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
// Snappy Compression
std::string SNAPPY_encode(std::vector<kv> kvs);
kv* SNAPPY_decode(string filepath);

// SIMD Functionality
std::string SIMD_encode(std::vector<kv> kvs);
kv* SIMD_decode(string filepath);

//Basic RLE for Testing
string RLE_encode(vector<kv> kvs);
kv* RLE_decode(string filepath);