#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "snappy.h"


typedef enum Status {
    OK,
    ERR_FOPEN,
    ERR_FREAD,
    ERR_FWRITE,
} Status;

typedef enum EncType {
    RLE,
    DELTA,
} EncType;

typedef struct RLEPair {
    int data;
    int count;
} RLEPair;

// typedef struct RLECursor {
// 	int rel_len;
// 	RLEPair *enc;
// }

typedef struct FileNode {
    EncType etype;
    char *filepath;
    FILE *fp;
    void *cursor;
    int eofreached;
    struct FileNode *prev;
    struct FileNode *next;
} FileNode;

extern int compressed_file_count;

Status rle_delta_file_encode(const char *filepath, char **new_file);
// int *rlestreamdecode(const char *filepath, size_t seg_len, size_t *num_res);
int *rle_delta_stream_decode(const char *filepath, size_t seg_len, size_t *num_res);
int *rle_delta_f2m_decode(const char *filepath, size_t seg_len, size_t *num_res);

// Add functionality for Snappy
std::string snappy_array_encode(char* buffer, size_t data_size);
const char *snappy_array_decode(std::string filepath);
