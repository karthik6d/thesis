#include <stdio.h>
#include <stdlib.h>
#include <string.h>


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

Status rle_delta_file_encode(char *filepath);
int *rle_delta_stream_decode(char *filepath, size_t seg_len, size_t *num_res);

// int *deltastreamdecode(char *filepath, size_t seg_len, size_t *num_res);





