#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
// #include <rand.h>
#include <limits.h>


int main(int argc, char **argv) {
	if (argc != 5) {
		printf("Usage: ./gensortedints <base> <number> <max_change> <filename>\n");
		return 1;
	}

	srand(265);

	// int low = atoi(argv[1]);
	// int high = atoi(argv[2]);
	int base = atoi(argv[1]);
	size_t num = (size_t)atol(argv[2]);
	size_t modulus = atoi(argv[3]);

	char *outfile_name = (char *)malloc(8 + strlen(argv[4]));
	sprintf(outfile_name, "../raw/%s", argv[4]);
	outfile_name[7 + strlen(argv[4])] = '\0';

	FILE *outfile = fopen(outfile_name, "w");
	if (outfile == NULL) {
		free(outfile_name);
		printf("Failed to open outfile\n");
		return 2;
	}

	size_t num_written = 0;

	int *write_buffer = (int *)malloc(getpagesize());
	size_t ints_per_page = getpagesize()/sizeof(int);

	int cur = base;	

	while (num_written < num) {
		size_t num_to_write = ((num - num_written) < ints_per_page)*(num - num_written) +
			((num - num_written) >= ints_per_page)*ints_per_page;

		for (size_t i = 0; i < num_to_write; ++i) {
			write_buffer[i] = cur;
			cur += rand() % modulus;
			printf("cur = %i\n", cur);
		}

		if (fwrite(write_buffer, sizeof(int), num_to_write, outfile) < num_to_write) {
			free(write_buffer);
			fclose(outfile);
			printf("Failed to write properly to outfile\n");
			return 3;
		}

		num_written += num_to_write;
	}

	printf("Test file created - %s\n", outfile_name);

	free(write_buffer);
	fclose(outfile);
	free(outfile_name);

	return 0;
}









