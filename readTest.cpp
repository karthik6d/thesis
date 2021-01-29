#include <iostream>
#include <vector>
#include "snappy.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace std;

// Takes a uncompressed file and compresses it using the snappy compression scheme
void compressFile(FILE* f, string new_path, size_t length) {
    char* data = (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);

    string compressed;
    snappy::Compress(data, length, &compressed);

    ofstream data_file(new_path, ios::binary);
    data_file.write(compressed.c_str(), compressed.size());
    data_file.close();
}

// Reads blocks of data from a compressed file
void loadCompressedBlock(FILE* f, char* data, size_t block_size) {
    string uncompressed;
    snappy::Uncompress(data, block_size, &uncompressed);            
}

// Reads blocks of data from disk to memory of size block size
char* loadUncompressedBlock(FILE* f, size_t block_size) {
    char* data = (char*)malloc(block_size);
    fread (data, sizeof(char), block_size, f);

    return data;
}

int main() {
    string query_file = "data/sample.dat";
    string compressed_query_file = "data/sample_compressed.dat";
    clock_t start;

    //Open the uncompressed read file and get the size
    FILE* f = fopen(query_file.c_str(), "r");
    struct stat st;
    stat(query_file.c_str(), &st);
    size_t length = st.st_size;

    cout << "Size of the uncompressed file: " << length << endl;

    //Compress the file
    compressFile(f, compressed_query_file, length);

    //Open the compressed read file and get the size
    FILE* f1 = fopen(compressed_query_file.c_str(), "r");
    struct stat st1;
    stat(compressed_query_file.c_str(), &st1);
    size_t compressed_length = st1.st_size;

    cout << "Size of the compressed file: " << compressed_length << endl;

    // Record the time it takes to read uncompressed files from disk
    start = clock();
    char* uncompressed_data = loadUncompressedBlock(f, length);
    double duration = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Uncompressed Read: " << duration << endl;

    //Record the time it takes to load compressed files from disk
    start = clock();
    char* compressed_data = loadUncompressedBlock(f1, compressed_length);
    double duration1 = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Compressed Read: " << duration1 << endl;

    //Record the time it takes to decompress the compressed data
    start = clock();
    loadCompressedBlock(f1, compressed_data, compressed_length);
    double duration2 = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Decompression: " << duration2 << endl;

    //Print the total time for reading compressed file and decompressing
    cout << "Total Execution time of full decompression: " << duration1 + duration2 << endl;

    //Print the compressed to uncompressed size ration
    cout << "Compressed to Uncompressed Ratio: " << (double)compressed_length / (double)length;

    return 1;
}