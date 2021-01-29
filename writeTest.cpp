#include <iostream>
#include <vector>
#include "snappy.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace std;

// Takes data and compresses it
string compressData(char* data, size_t length) {
    string compressed;
    snappy::Compress(data, length, &compressed);

    return compressed;
}

// Writes compressed data to disk
void writeCompressedData(string compressed_data, string new_path){
    ofstream data_file(new_path, ios::binary);
    data_file.write(compressed_data.c_str(), compressed_data.size());
    data_file.close();
}

// Writes uncompressed data to disk
void writeUncompressedData(char* data, string new_path, size_t length) {
    ofstream data_file(new_path, ios::binary);
    data_file.write(data, length);
    data_file.close();
}

int main() {
    string data_file = "data/sample.dat";
    string uncompressed_data_file = "data/uncompressed_data.dat";
    string compressed_query_file = "data/compressed_data.dat";
    clock_t start;

    //Open the uncompressed data file and get the size
    FILE* f = fopen(data_file.c_str(), "r");
    struct stat st;
    stat(data_file.c_str(), &st);
    size_t length = st.st_size;

    //Get the data from the uncompressed data file
    char* data = (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);

    cout << "Size of the uncompressed file: " << length << endl;

    //Time it takes to write the uncompressed data to disk
    start = clock();
    writeUncompressedData(data, uncompressed_data_file, length);
    double duration = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Uncompressed Write: " << duration << endl;

    //Time it takes to compress the data
    start = clock();
    string compressed_data = compressData(data, length);
    double duration1 = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Compressing data: " << duration1 << endl;

    cout << "Size of compressed data: " << compressed_data.size() << endl;

    //Time it takes to write compressed data to disk
    start = clock();
    writeCompressedData(compressed_data, compressed_query_file);
    double duration2 = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of writing Compressed Data: " << duration2 << endl;

    //Time it takes for full compression write
    cout << "Execution Time of full compression write: " << duration1 + duration2 << endl;

    //Compression Ratio
    cout << "Compressed to Uncompressed Ratio: " << (double)compressed_data.size() / (double)length;

    return 1;
}