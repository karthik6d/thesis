#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "compression.h"
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include "snappy.h"

using namespace std;

int compressed_file_count = 0;

FileNode *openfiles = NULL;


// TODOs handle error handling better
string snappy_array_encode(char* buffer, size_t data_size) {
    string new_path = "enc/";
    new_path += to_string(compressed_file_count++);
    new_path += ".dat";

    ofstream data_file(new_path, ios::binary);

    string compressed_data;
    snappy::Compress(buffer, data_size, &compressed_data);
    
    data_file.write(compressed_data.c_str(), compressed_data.size());
    data_file.close();

    return new_path;
}

const char *snappy_array_decode(string filepath){
    ifstream f(filepath, ios::ate | ios::binary);
    int length = f.tellg();

    f.seekg(0, f.beg);

    //Prepare the buffer
    char* buf = (char*) malloc(length);
    f.read((char*)buf, length);
    string uncompressed_data;

    // Uncompress the data
    snappy::Uncompress(buf, length, &uncompressed_data);

    const char* data = uncompressed_data.c_str();
    free(buf);
    
    return data;
}

Status rle_delta_file_encode(const char *filepath, char **new_file) {
    // printf("rdfe check 1\n");
    //printf("FILEPATH: %s\n", filepath);

    FILE *infile = fopen(filepath, "r");

    if (infile == NULL) {
        // File opening error
        printf("Error opening infile\n");
        return ERR_FOPEN;
    }

    // size_t ints_per_page = getpagesize()/sizeof(int);
    size_t page_size = 8 * getpagesize();
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

    std::string new_path = "enc/";
    new_path += to_string(compressed_file_count++);
    new_path += ".enc";

    char *new_filename = (char *)malloc(new_path.length() + 1);
    strcpy(new_filename, new_path.c_str());

    FILE *outfile = fopen(new_filename, "w");
    if (outfile == NULL) {
        cout << "Error opening outputfile - %s" << new_filename << " Error - %s" << strerror(errno) << endl;
        free(new_filename);
        free(stream);
        fclose(infile);
        return ERR_FOPEN;
    }

    // free(new_filename);

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

        *new_file = new_filename;
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
    // int failed = 0;

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

    *new_file = new_filename;
    return OK;
}

// int *rlestreamdecode(const char *filepath, size_t seg_len, size_t *num_res) {
//     printf("rsd check 1\n");

//     FileNode *targetfile = openfiles;

//     while (targetfile != NULL) {
//         if (!strcmp(filepath, targetfile -> filepath)) {
//             break;
//         }

//         targetfile = targetfile -> next;
//     }

//     printf("rsd check 2\n");

//     if (targetfile == NULL && openfiles == NULL) {
//         printf("rsd check 2.1.1\n");

//         FILE *infile = fopen(filepath, "r");

//         targetfile = openfiles = (FileNode *)malloc(sizeof(FileNode));
//         targetfile -> prev = targetfile -> next = NULL;
//         targetfile -> filepath = strdup(filepath);
//         targetfile -> etype = RLE;
//         targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

//         printf("rsd check 2.2.1 - %s, %p\n", filepath, infile);

//         size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

//         printf("rsd check 2.3.1\n");

//         targetfile -> eofreached = num_read != seg_len;
//         ((RLEPair *)(targetfile -> cursor)) -> data = num_read;

//         targetfile -> fp = infile;
//     }
//     else if (targetfile == NULL && openfiles != NULL) {
//         printf("rsd check 2.1.2\n");

//         FILE *infile = fopen(filepath, "r");

//         targetfile = (FileNode *)malloc(sizeof(FileNode));
//         targetfile -> prev = NULL;
//         targetfile -> next = openfiles;
//         targetfile -> filepath = strdup(filepath);
//         targetfile -> etype = RLE;
//         targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

//         size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

//         targetfile -> eofreached = num_read != seg_len;
//         ((RLEPair *)(targetfile -> cursor)) -> data = num_read;

//         targetfile -> fp = infile;

//         openfiles = targetfile;
//     }

//     printf("rsd check 3\n");

//     int cursor_len = ((RLEPair *)(targetfile -> cursor)) -> data;

//     if (cursor_len < seg_len && !(targetfile -> eofreached)) {
//         targetfile -> cursor = (void *)realloc(targetfile -> cursor, (cursor_len + seg_len + 1)*sizeof(RLEPair));

//         size_t num_read = fread((RLEPair *)(targetfile -> cursor) + 1 + cursor_len,
//                                 sizeof(RLEPair), seg_len, targetfile -> fp);

//         ((RLEPair *)(targetfile -> cursor)) -> data = cursor_len + num_read;

//         targetfile -> eofreached = (num_read != seg_len);
//     }

//     printf("rsd check 4\n");

//     int *toReturn = (int *)malloc(seg_len*sizeof(int));

//     size_t write_ctr = 0;
//     size_t read_ctr = 0;

//     RLEPair *enc = (RLEPair *)(targetfile -> cursor) + 1;

//     cursor_len = ((RLEPair *)(targetfile -> cursor)) -> data;

//     printf("rsd check 5\n");

//     for (size_t i = 0; i < seg_len; ++i) {
//         toReturn[i] = (enc + read_ctr) -> data;
//         ++write_ctr;
//         (enc + read_ctr) -> count = (enc + read_ctr) -> count - 1;
//         read_ctr += !((enc + read_ctr) -> count);
//         i += (seg_len - read_ctr)*(read_ctr == cursor_len);
//     }

//     // printf("HELLO\n");
//     printf("rsd check 6\n");
//     // printf("WHAT\n");

//     // for(size_t i = 0; i < seg_len; i++){
//     //     printf("BEFORE");
//     //     //printf("Hello: %d\t", toReturn[i]);
//     // }

//     memmove(((RLEPair *)(targetfile -> cursor)) + 1, ((RLEPair *)(targetfile -> cursor)) + 1 + read_ctr,
//             sizeof(RLEPair)*(cursor_len - read_ctr));

//     printf("rsd check 7\n");

//     ((RLEPair *)(targetfile -> cursor)) -> data = cursor_len - read_ctr;

//     if (write_ctr < seg_len) {
//         printf("rsd check 7.1\n");
//         toReturn = (int *)realloc(toReturn, write_ctr*sizeof(int));

//         fclose(targetfile -> fp);
//         free(targetfile -> filepath);
//         free(targetfile -> cursor);

//         printf("rsd check 7.2\n");

//         if (targetfile -> next == NULL && targetfile -> prev == NULL) {
//             openfiles = NULL;
//         }
//         else if (targetfile -> next != NULL && targetfile -> prev == NULL) {
//             openfiles = targetfile -> next;
//             openfiles -> prev = NULL;
//         }
//         else if (targetfile -> next == NULL && targetfile -> prev != NULL) {
//             targetfile -> prev -> next = NULL;
//         }
//         else {
//             targetfile -> next -> prev = targetfile -> prev;
//             targetfile -> prev -> next = targetfile -> next;
//         }

//         printf("rsd check 7.3\n");

//         free(targetfile);
//     }

//     printf("rsd check 8\n");

//     *num_res = write_ctr;

//     printf("rsd check 9\n");

//     return toReturn;
// }


int *rle_delta_stream_decode(const char *filepath, size_t seg_len, size_t *num_res) {
    // printf("rdsd check 1\n");

    FileNode *targetfile = openfiles;

    while (targetfile != NULL) {
        if (!strcmp(filepath, targetfile -> filepath)) {
            break;
        }

        targetfile = targetfile -> next;
    }

    // printf("rdsd check 2\n");

    if (targetfile == NULL && openfiles == NULL) {
        // printf("rdsd check 2.1.1\n");

        FILE *infile = fopen(filepath, "r");

        targetfile = openfiles = (FileNode *)malloc(sizeof(FileNode));
        targetfile -> prev = targetfile -> next = NULL;
        targetfile -> filepath = strdup(filepath);
        targetfile -> etype = RLE;
        targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

        // printf("rdsd check 2.2.1 - %s, %p\n", filepath, infile);

        size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

        // printf("rdsd check 2.3.1\n");

        targetfile -> eofreached = num_read != seg_len;

        ((RLEPair *)(targetfile -> cursor)) -> data = (((RLEPair *)(targetfile -> cursor)) + 1) -> data;
        ((RLEPair *)(targetfile -> cursor)) -> count = num_read;
        (((RLEPair *)(targetfile -> cursor)) + 1) -> data = 0;
        (((RLEPair *)(targetfile -> cursor)) + 1) -> count = 1;

        // ((RLEPair *)(targetfile -> cursor)) -> data = num_read;

        targetfile -> fp = infile;
    }
    else if (targetfile == NULL && openfiles != NULL) {
        // printf("rdsd check 2.1.2\n");

        FILE *infile = fopen(filepath, "r");

        targetfile = (FileNode *)malloc(sizeof(FileNode));
        targetfile -> prev = NULL;
        targetfile -> next = openfiles;
        targetfile -> filepath = strdup(filepath);
        targetfile -> etype = RLE;
        targetfile -> cursor = (void *)malloc((seg_len + 1)*sizeof(RLEPair));

        size_t num_read = fread(((RLEPair *)(targetfile -> cursor)) + 1, sizeof(RLEPair), seg_len, infile);

        targetfile -> eofreached = num_read != seg_len;

        ((RLEPair *)(targetfile -> cursor)) -> data = (((RLEPair *)(targetfile -> cursor)) + 1) -> data;
        ((RLEPair *)(targetfile -> cursor)) -> count = num_read;
        (((RLEPair *)(targetfile -> cursor)) + 1) -> data = 0;
        (((RLEPair *)(targetfile -> cursor)) + 1) -> count = 1;

        // ((RLEPair *)(targetfile -> cursor)) -> data = num_read;

        targetfile -> fp = infile;

        openfiles = targetfile;
    }

    // printf("rdsd check 3\n");

    int cursor_len = ((RLEPair *)(targetfile -> cursor)) -> count;

    if (cursor_len < seg_len && !(targetfile -> eofreached)) {
        targetfile -> cursor = (void *)realloc(targetfile -> cursor, (cursor_len + seg_len + 1)*sizeof(RLEPair));

        size_t num_read = fread((RLEPair *)(targetfile -> cursor) + 1 + cursor_len,
                                sizeof(RLEPair), seg_len, targetfile -> fp);

        ((RLEPair *)(targetfile -> cursor)) -> count = cursor_len + num_read;

        targetfile -> eofreached = (num_read != seg_len);
    }

    // printf("rdsd check 4\n");

    int *toReturn = (int *)malloc(seg_len*sizeof(int));

    size_t write_ctr = 0;
    size_t read_ctr = 0;

    RLEPair *tracker = (RLEPair *)(targetfile -> cursor);
    RLEPair *enc = (RLEPair *)(targetfile -> cursor) + 1;

    cursor_len = ((RLEPair *)(targetfile -> cursor)) -> count;

    // printf("rdsd check 5\n");

    for (size_t i = 0; i < seg_len; ++i) {
        tracker -> data = toReturn[i] = (enc + read_ctr) -> data + (tracker -> data);
        ++write_ctr;
        (enc + read_ctr) -> count = (enc + read_ctr) -> count - 1;
        read_ctr += !((enc + read_ctr) -> count);
        i += (seg_len - read_ctr)*(read_ctr == cursor_len);
    }

    // printf("rdsd check 6\n");

    memmove(((RLEPair *)(targetfile -> cursor)) + 1, ((RLEPair *)(targetfile -> cursor)) + 1 + read_ctr,
            sizeof(RLEPair)*(cursor_len - read_ctr));

    // printf("rdsd check 7\n");

    ((RLEPair *)(targetfile -> cursor)) -> count = cursor_len - read_ctr;

    // if (write_ctr < seg_len) {
        // printf("rdsd check 7.1\n");
        toReturn = (int *)realloc(toReturn, write_ctr*sizeof(int));

        fclose(targetfile -> fp);
        free(targetfile -> filepath);
        free(targetfile -> cursor);

        // printf("rdsd check 7.2\n");

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

        // printf("rdsd check 7.3\n");

        free(targetfile);
    // }

    // printf("rdsd check 8\n");

    *num_res = write_ctr;

    // printf("rdsd check 9\n");

    return toReturn;
}


int *rle_delta_f2m_decode(const char *filepath, size_t seg_len, size_t *num_res) {
    FILE *infile = fopen(filepath, "r");

    RLEPair *read_buffer = (RLEPair *)calloc(sizeof(RLEPair), seg_len + 1);

    size_t elts_read = fread(read_buffer, sizeof(RLEPair), seg_len, infile);

    read_buffer -> data = (read_buffer + 1) -> data;
    read_buffer -> count = elts_read;
    (read_buffer + 1) -> data = 0;
    (read_buffer + 1) -> count = 1;

    int *toReturn = (int *)malloc(seg_len*sizeof(int));

    RLEPair *enc = read_buffer + 1;

    size_t write_ctr = 0;
    size_t read_ctr = 0;

    for (int i = 0; i < read_buffer -> count; ++i) {
        read_buffer -> data = toReturn[i] = (read_buffer -> data) + (enc -> data);
        enc -> count = (enc -> count) - 1;
        read_ctr += ((enc -> count) == 0);
        enc = enc + ((enc -> count) == 0);
        ++write_ctr;
        i += (seg_len - read_ctr)*(read_ctr == elts_read);
    }

    toReturn = (int *)realloc(toReturn, write_ctr*sizeof(int));

    *num_res = write_ctr;

    fclose(infile);
    free(read_buffer);

    return toReturn;
}