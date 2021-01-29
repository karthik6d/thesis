#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "encode.h"


FileNode *openfiles = NULL;


int main(void) {
	char *filestr = (char *)malloc(100*sizeof(char));
	strcpy(filestr, "../raw/testints01.raw");
	Status status = rle_delta_file_encode(filestr);

	printf("Status = %i\n", status);

	return 0;
}


Status rle_delta_file_encode(char *filepath) {
	// printf("rdfe check 1\n");

	FILE *infile = fopen(filepath, "r");

	if (infile == NULL) {
		// File opening error
		printf("Error opening infile\n");
		return ERR_FOPEN;
	}

	// size_t ints_per_page = getpagesize()/sizeof(int);
	size_t page_size = getpagesize();
	size_t ints_per_page = page_size/sizeof(int);

	int *stream = (int *)malloc(page_size);

	size_t ints_read = fread(stream, sizeof(int), ints_per_page, infile);

	if (ints_read < ints_per_page) {
		if (!feof(infile)) {
			// Fewer ints read than requested, but not end of file
			printf("Error reading first set of ints\n");
			free(stream);
			fclose(infile);
			return ERR_FREAD;
		}
	}

	if (!ints_read) {
		printf("No ints to read\n");
		free(stream);
		fclose(infile);
		return OK;
	}

	char *path_copy_base = strdup(filepath);
	char *path_copy = path_copy_base;
	// strsep(&path_copy, "./raw/");

	size_t filename_len = strlen(path_copy);
	char *new_filename = (char *)malloc(8 + filename_len);
	sprintf(new_filename, "../enc/%s", path_copy + 7);
	new_filename[strlen(new_filename) - 3] = 'e';
	new_filename[strlen(new_filename) - 2] = 'n';
	new_filename[strlen(new_filename) - 1] = 'c';
	// new_filename[7 + filename_len] = '\0';

	printf("new_filename = %s\n", new_filename);

	free(path_copy_base);

	FILE *outfile = fopen(new_filename, "w");
	if (outfile == NULL) {
		printf("Error opening outfile - %s\n", new_filename);
		free(new_filename);
		free(stream);
		fclose(infile);
		return ERR_FOPEN;
	}

	free(new_filename);

	if (ints_read == 1) {
		RLEPair pair;
		(&pair) -> data = stream[0];
		(&pair) -> count = 0;

		if (fwrite(&pair, sizeof(RLEPair), 1, outfile) != 1) {
			free(stream);
			fclose(infile);
			fclose(outfile);
			return ERR_FWRITE;
		}

		free(stream);
		fclose(infile);
		fclose(outfile);
		return OK;
	}

	int base_int = stream[0];
	int prev_int = base_int;

	RLEPair *write_buffer = (RLEPair *)calloc(2*ints_per_page, sizeof(RLEPair));
	write_buffer -> data = base_int;
	write_buffer -> count = 0;

	size_t write_ctr = 1;

	(write_buffer + write_ctr) -> data = stream[1] - prev_int;
	(write_buffer + write_ctr) -> count = 1;
	prev_int = stream[1];

	size_t read_ctr = 2;

	int inprog = 1;
	int failed = 0;

	while (inprog) {
		while (read_ctr < ints_read) {
			int new_delta = stream[read_ctr] - prev_int;
			int advance = (new_delta != (write_buffer + write_ctr) -> data);
			write_ctr += advance;
			(write_buffer + write_ctr) -> data = new_delta;
			(write_buffer + write_ctr) -> count = (write_buffer + write_ctr) -> count + 1;
			prev_int = stream[read_ctr];
			++read_ctr;
		}

		if (write_ctr >= ints_per_page) {
			if (fwrite(write_buffer, sizeof(RLEPair), ints_per_page, outfile) < ints_per_page) {
				free(stream);
				free(write_buffer);
				fclose(infile);
				fclose(outfile);
				return ERR_FWRITE;
			}

			memmove(write_buffer, write_buffer + ints_per_page, sizeof(RLEPair)*(write_ctr + 1 - ints_per_page));
			memset(write_buffer + write_ctr - ints_per_page + 1, 0, sizeof(RLEPair)*(3*ints_per_page - write_ctr - 1));
			write_ctr -= ints_per_page;
		}

		ints_read = fread(stream, sizeof(int), ints_per_page, infile);

		if (ints_read < ints_per_page) {
			if (!feof(infile)) {
				free(stream);
				free(write_buffer);
				fclose(infile);
				fclose(outfile);
				return ERR_FREAD;
			}

			--inprog;
		}
	}

	while (read_ctr < ints_read) {
		int new_delta = stream[read_ctr] - prev_int;
		int advance = (new_delta != (write_buffer + write_ctr) -> data);
		write_ctr += advance;
		(write_buffer + write_ctr) -> data = new_delta;
		(write_buffer + write_ctr) -> count = (write_buffer + write_ctr) -> count + 1;
		prev_int = stream[read_ctr];
		++read_ctr;
	}

	if (fwrite(write_buffer, sizeof(RLEPair), write_ctr + 1, outfile) < (write_ctr + 1)) {
		free(stream);
		free(write_buffer);
		fclose(infile);
		fclose(outfile);
		return ERR_FWRITE;
	}

	free(stream);
	free(write_buffer);
	fclose(infile);
	fclose(outfile);
	return OK;
}

int *rle_delta_stream_decode(char *filepath, size_t seg_len, size_t *num_res) {
	printf("rsd check 1\n");

	FileNode *targetfile = openfiles;

	while (targetfile != NULL) {
		if (!strcmp(filepath, targetfile -> filepath)) {
			break;
		}

		targetfile = targetfile -> next;
	}

	printf("rsd check 2\n");

	if (targetfile == NULL && openfiles == NULL) {
		printf("rsd check 2.1.1\n");

		FILE *infile = fopen(filepath, "r");

		targetfile = openfiles = (FileNode *)malloc(sizeof(FileNode));
		targetfile -> prev = targetfile -> next = NULL;
		targetfile -> filepath = strdup(filepath);
		targetfile -> etype = RLE;
		targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

		printf("rsd check 2.2.1 - %s, %p\n", filepath, infile);

		size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

		printf("rsd check 2.3.1\n");

		targetfile -> eofreached = num_read != seg_len;
		((RLEPair *)(targetfile -> cursor)) -> data = num_read;

		targetfile -> fp = infile;
	}
	else if (targetfile == NULL && openfiles != NULL) {
		printf("rsd check 2.1.2\n");

		FILE *infile = fopen(filepath, "r");

		targetfile = (FileNode *)malloc(sizeof(FileNode));
		targetfile -> prev = NULL;
		targetfile -> next = openfiles;
		targetfile -> filepath = strdup(filepath);
		targetfile -> etype = RLE;
		targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

		size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

		targetfile -> eofreached = num_read != seg_len;
		((RLEPair *)(targetfile -> cursor)) -> data = num_read;

		targetfile -> fp = infile;

		openfiles = targetfile;
	}

	printf("rsd check 3\n");

	int cursor_len = ((RLEPair *)(targetfile -> cursor)) -> data;

	if (cursor_len < seg_len && !(targetfile -> eofreached)) {
		targetfile -> cursor = (void *)realloc(targetfile -> cursor, (cursor_len + seg_len + 1)*sizeof(RLEPair));

		size_t num_read = fread((RLEPair *)(targetfile -> cursor) + 1 + cursor_len,
			sizeof(RLEPair), seg_len, targetfile -> fp);

		((RLEPair *)(targetfile -> cursor)) -> data = cursor_len + num_read;

		targetfile -> eofreached = (num_read != seg_len);
	}

	printf("rsd check 4\n");

	int *toReturn = (int *)malloc(seg_len*sizeof(int));

	size_t write_ctr = 0;
	size_t read_ctr = 0;

	RLEPair *enc = (RLEPair *)(targetfile -> cursor) + 1;

	cursor_len = ((RLEPair *)(targetfile -> cursor)) -> data;

	printf("rsd check 5\n");

	for (size_t i = 0; i < seg_len; ++i) {
		toReturn[i] = (enc + read_ctr) -> data;
		++write_ctr;
		(enc + read_ctr) -> count = (enc + read_ctr) -> count - 1;
		read_ctr += !((enc + read_ctr) -> count);
		i += (seg_len - read_ctr)*(read_ctr == cursor_len);
	}

	printf("rsd check 6\n");

	memmove(((RLEPair *)(targetfile -> cursor)) + 1, ((RLEPair *)(targetfile -> cursor)) + 1 + read_ctr,
		sizeof(RLEPair)*(cursor_len - read_ctr));

	((RLEPair *)(targetfile -> cursor)) -> data = cursor_len - read_ctr;

	if (write_ctr < seg_len) {
		toReturn = (int *)realloc(toReturn, write_ctr*sizeof(int));

		fclose(targetfile -> fp);
		free(targetfile -> filepath);
		free(targetfile -> cursor);

		if (targetfile -> next == NULL && targetfile -> prev == NULL) {
			openfiles = NULL;
		}
		else if (targetfile -> next != NULL && targetfile -> prev == NULL) {
			openfiles = targetfile -> next;
			openfiles -> prev = NULL;
		}
		else if (targetfile -> next == NULL && targetfile -> prev != NULL) {
			targetfile -> prev -> next = NULL;
		}
		else {
			targetfile -> next -> prev = targetfile -> prev;
			targetfile -> prev -> next = targetfile -> next;
		}

		free(targetfile);
	}

	*num_res = write_ctr;

	printf("rsd check 7\n");

	return toReturn;
}



// Status varint_delta_file_encode(char *filepath) {
// 	FILE *infile = fopen(filepath, "r");

// 	if (infile == NULL) {
// 		// File opening error
// 		printf("Error opening infile\n");
// 		return ERR_FOPEN;
// 	}

// 	// size_t ints_per_page = getpagesize()/sizeof(int);
// 	size_t page_size = getpagesize();
// 	size_t ints_per_page = page_size/sizeof(int);

// 	int *stream = (int *)malloc(page_size);

// 	size_t ints_read = fread(stream, sizeof(int), ints_per_page, infile);

// 	if (ints_read < ints_per_page) {
// 		if (!feof(infile)) {
// 			// Fewer ints read than requested, but not end of file
// 			printf("Error reading first set of ints\n");
// 			free(stream);
// 			fclose(infile);
// 			return ERR_FREAD;
// 		}
// 	}

// 	if (!ints_read) {
// 		printf("No ints to read\n");
// 		free(stream);
// 		fclose(infile);
// 		return OK;
// 	}

// 	char *path_copy_base = strdup(filepath);
// 	char *path_copy = path_copy_base;
// 	// strsep(&path_copy, "./raw/");

// 	size_t filename_len = strlen(path_copy);
// 	char *new_filename = (char *)malloc(8 + filename_len);
// 	sprintf(new_filename, "../enc/%s", path_copy + 7);
// 	new_filename[strlen(new_filename) - 3] = 'e';
// 	new_filename[strlen(new_filename) - 2] = 'n';
// 	new_filename[strlen(new_filename) - 1] = 'c';
// 	// new_filename[7 + filename_len] = '\0';

// 	printf("new_filename = %s\n", new_filename);

// 	free(path_copy_base);

// 	FILE *outfile = fopen(new_filename, "w");
// 	if (outfile == NULL) {
// 		printf("Error opening outfile - %s\n", new_filename);
// 		free(new_filename);
// 		free(stream);
// 		fclose(infile);
// 		return ERR_FOPEN;
// 	}

// 	free(new_filename);

// 	if (ints_read == 1) {
// 		int i = stream[0];

// 		if (fwrite(&i, sizeof(int), 1, outfile) != 1) {
// 			free(stream);
// 			fclose(infile);
// 			fclose(outfile);
// 			printf("Error writing single int result to outfile\n");
// 			return ERR_FWRITE;
// 		}

// 		free(stream);
// 		fclose(infile);
// 		fclose(outfile);
// 		return OK;
// 	}

// 	size_t read_ctr = 1;

// 	int inprog = 1;

// 	while (inprog) {
// 		while (read_ctr < ints_read) {
			
// 		}
// 	}
// }







// int *deltastreamdecode(char *filepath, size_t seg_len, size_t *num_res) {
// 	printf("dsd check 1\n");

// 	FileNode *targetfile = openfiles;

// 	while (targetfile != NULL) {
// 		if (!strcmp(filepath, targetfile -> filepath)) {
// 			break;
// 		}

// 		targetfile = targetfile -> next;
// 	}

// 	printf("rsd check 2\n");

// 	if (targetfile == NULL && openfiles == NULL) {
// 		printf("rsd check 2.1.1\n");

// 		FILE *infile = fopen(filepath, "r");

// 		targetfile = openfiles = (FileNode *)malloc(sizeof(FileNode));
// 		targetfile -> prev = targetfile -> next = NULL;
// 		targetfile -> filepath = strdup(filepath);
// 		targetfile -> etype = RLE;
// 		targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

// 		printf("rsd check 2.2.1 - %s, %p\n", filepath, infile);

// 		size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

// 		printf("rsd check 2.3.1\n");

// 		targetfile -> eofreached = num_read != seg_len;
// 		((RLEPair *)(targetfile -> cursor)) -> data = num_read;

// 		targetfile -> fp = infile;
// 	}
// 	else if (targetfile == NULL && openfiles != NULL) {
// 		printf("rsd check 2.1.2\n");

// 		FILE *infile = fopen(filepath, "r");

// 		targetfile = (FileNode *)malloc(sizeof(FileNode));
// 		targetfile -> prev = NULL;
// 		targetfile -> next = openfiles;
// 		targetfile -> filepath = strdup(filepath);
// 		targetfile -> etype = RLE;
// 		targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

// 		size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

// 		targetfile -> eofreached = num_read != seg_len;
// 		((RLEPair *)(targetfile -> cursor)) -> data = num_read;

// 		targetfile -> fp = infile;

// 		openfiles = targetfile;
// 	}

// 	printf("rsd check 3\n");


// }



















