#include <iostream>
#include <bits/stdc++.h> 
#include <vector>
#include "snappy.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace std;

// Reads blocks of data from a compressed file
const char* loadCompressedBlock(FILE* f, size_t block_size) {
    char* data = (char*)malloc(block_size);
    fread (data,sizeof(char),block_size,f);
    string uncompressed;
    snappy::Uncompress(data, block_size, &uncompressed);
    return uncompressed.c_str();              
}

// Reads blocks of data from disk to memory of size block size
char* loadUncompressedBlock(FILE* f, size_t block_size) {
    char* data = (char*)malloc(block_size);
    fread (data, sizeof(char), block_size, f);
    return data;
}

vector breakUncompressedFile(FILE* f, size_t length, int block_size) {
    string base_path = "data/uncompressed";
    vector<string> paths;
    
    char* data = (char*)malloc(length);
    fread (data, sizeof(char), length, f);

    size_t curr_amount_read = 0;
    int counter = 0;
    while(curr_amount_read < length) {
        int amount_left = (int)length - (int)curr_amount_read;
        size_t amount_to_write = min(amount_left, block_size);

        string new_path = base_path + "/file" + to_string(counter) + ".txt";
        paths.push_back(new_path);
        ofstream data_file(new_path, ios::binary);
        data_file.write(data+curr_amount_read, amount_to_write);
        data_file.close();

        curr_amount_read += amount_to_write;
    }
    fclose(f);
    return paths;
}


// Takes a uncompressed file and compresses it using the snappy compression scheme
void compressFile(FILE* f, string new_path, size_t length, size_t block_size) {
    char* data = (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);

    string compressed;
    snappy::Compress(data, length, &compressed);

    ofstream data_file(new_path, ios::binary);
    data_file.write(compressed.c_str(), compressed.size());
    data_file.close();
}

int main() {
    int block_size = 100000;
    string query_file = "data/sample.txt";

    // Open the uncompressed file and get the size
    FILE* f = fopen(query_file.c_str(), "r");
    struct stat st;
    stat(query_file.c_str(), &st);
    size_t length = st.st_size;

    // Compress the file 
    string compressed_file = "data/sample_compressed.txt";
    compressFile(f, compressed_file, length);

    // Open the compressed file and get the size
    FILE* f1 = fopen(compressed_file.c_str(), "r");
    //struct stat st;
    stat(compressed_file.c_str(), &st);
    size_t compressed_length = st.st_size;

    // Perform the reads for the uncompressed version
    size_t curr_amount_read = 0;
    int num_uncompressed_reads = 0;
    cout << "File Size: " << length << endl;
    while(curr_amount_read < length) {
        int amount_left = (int)length - (int)curr_amount_read;
        size_t amount_to_read = min(amount_left, block_size);
        char* data = loadUncompressedBlock(f, amount_to_read);
        curr_amount_read += amount_to_read;
        num_uncompressed_reads++;
    }
    fclose(f);
    cout << "Number of reads needed: " << num_uncompressed_reads << endl;

    // Perform the reads for the compressed version
    size_t curr_amount_read_compressed = 0;
    int num_compressed_reads = 0;
    int total_size = 0;
    cout << "File Size: " << compressed_length << endl;
    while(curr_amount_read_compressed < compressed_length) {
        int amount_left = (int)compressed_length - (int)curr_amount_read_compressed;
        size_t amount_to_read = min(1000, amount_left);
        const char* data = loadCompressedBlock(f1, amount_to_read);
        // cout << data << endl;
        total_size += strlen(data);
        cout << total_size << endl;
        curr_amount_read_compressed += amount_to_read;
        num_compressed_reads++;
    }
    fclose(f1);
    cout << "Number of reads needed: " << num_compressed_reads << endl;

    return 1;
}