#include <iostream>
#include <vector>
#include "snappy.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace std;

const char* loadCompressedFile(string filepath, size_t length) {
    FILE* f = fopen(filepath.c_str(), "r");
    char* data = (char*)malloc(length);
    fread (data,sizeof(char),length,f);
    string uncompressed;
    snappy::Uncompress(data, length, &uncompressed);
    //free(data);
    //free(&uncompressed); 
    return uncompressed.c_str();              
}


char* loadUncompressedFile(string filepath, size_t length) {
    FILE* f = fopen(filepath.c_str(), "r");
    char* data = (char*)malloc(length);
    fread (data,sizeof(char),length,f);
    fclose(f);
    //free(data);
    return data;
}

void compressFile(string filepath, string new_path, size_t length) {
    FILE* f = fopen(filepath.c_str(), "r");
    char* data = (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);

    string compressed;
    snappy::Compress(data, length, &compressed);

    ofstream data_file(new_path, ios::binary);
    data_file.write(compressed.c_str(), compressed.size());
    data_file.close();
}

void writeUncompressedFile(string filepath, string new_path, size_t length) {
    FILE* f = fopen(filepath.c_str(), "r");
    char* data = (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);

    ofstream data_file(new_path, ios::binary);
    data_file.write(data, length);
    data_file.close();
    //free(data);
}

void writeTest(string query_file) {
    struct stat st;
    stat(query_file.c_str(), &st);
    size_t length = st.st_size;
    clock_t start;
    double duration;

    // Get the size of the file
    cout << "Size of the file: " << length << endl;

    //Compress a given data file
    start = clock();
    cout << "Compressing data file" << endl;
    string new_path = "data/sampleCompressed.txt";
    compressFile(query_file, new_path, length);
    duration = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Writing Compressed File: " << duration << endl;

    // Get the size of the compressed datafile
    stat(new_path.c_str(), &st);
    size_t new_length = st.st_size;
    cout << "Compressed Size: " << new_length << endl;

    // See the time is takes to read a file from disk that is compressed
    start = clock();
    cout << "starting basic write" << endl;
    string new_uncompressed_path = "data/sampleUncompressed.txt";
    writeUncompressedFile(query_file, new_uncompressed_path, length);
    duration = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Write: " << duration << endl;
}

void readTest(string query_file) {
    struct stat st;
    stat(query_file.c_str(), &st);
    size_t length = st.st_size;
    clock_t start;
    double duration;

    // Get the size of the file
    cout << "Size of the file: " << length << endl;

    // See the time is takes to read a file from disk that is uncompressed
    start = clock();
    cout << "starting basic uncompressed" << endl;
    char* uncompressed_data = loadUncompressedFile(query_file, length);
    duration = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Reads and Writes: " << duration << endl;

    //Compress a given data file
    cout << "Compressing data file" << endl;
    string new_path = "data/sampleCompressed.txt";
    compressFile(query_file, new_path, length);
    cout << "Finished compression" << endl;

    // Get the size of the compressed datafile
    stat(new_path.c_str(), &st);
    size_t new_length = st.st_size;
    cout << "Compressed Size: " << new_length << endl;

    // See the time is takes to read a file from disk that is compressed
    start = clock();
    cout << "starting basic compressed" << endl;
    const char* compressed_data = loadCompressedFile(new_path, new_length);
    duration = (clock() - start) / (double) CLOCKS_PER_SEC;
    cout << "Execution Time of Reads and Writes: " << duration << endl;

    //cout << "Strings equal: " << strcmp(uncompressed_data, compressed_data) << endl;
}


int main(int argc, char** argv)
{
    string query_file(argv[1]);
    char* read = argv[2];

    if(strcmp(read, "0") == 0) {
        readTest(query_file);
    } 
    else {
        writeTest(query_file);
    }

    return 1;
}