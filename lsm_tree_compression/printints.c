#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


int main(int argc, char **argv) {
	FILE *infile = fopen(argv[1], "r");

	int *stream = (int *)malloc(getpagesize());
	size_t ints_per_page = getpagesize()/sizeof(int);

	while (!feof(infile)) {
		size_t ints_read = fread(stream, sizeof(int), ints_per_page, infile);

		for (size_t i = 0; i < ints_read; ++i) {
			printf("%i;  ", stream[i]);
		}
	}

	printf("\n\n\n");

	fclose(infile);
}









