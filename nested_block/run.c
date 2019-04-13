#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "consts.h"

int operate(int32_t a, int32_t b, char op)
{
	switch (op) {
		case '=': return a == b;
		case '<': return a < b;
		case '>': return a > b;
	}
}

int indexof(char *elem, char *ls[])
{
	size_t i = 0;
	do {
		if (!strcmp(elem, ls[i])) {
			return i;
		}
	} while(ls[++i] != NULL);
	return -1;
}

void add_path(char *path, char *elem, char *filename)
{
	strcpy(filename, path);
	strcat(filename, elem);
	strcat(filename, ".bin");
}

/*
	filter corresponding values of a table
*/
int filter(size_t col_num, size_t col_num_out, char *cols[], char ops[], int32_t keys[],
					 size_t filter_num, int32_t *output[], char *cols_out[], char *path, char *filter_cols[])
{
	FILE *ifsp[col_num];
	int32_t *buffers[col_num];
	size_t SIZE, LEN;

	// Initialize input file, buffers, and read all data to the memory
	for (size_t i = 0; i < col_num; i ++) {
		char *filename = (char*)malloc(MAX_FILE_NAME);
		add_path(path, cols[i], filename);
		//printf("%s\n", filename);
		// open file and allocate memory for buffers
		ifsp[i] = fopen(filename, "rb");
		free(filename);
		if (i == 0) {
			fseek(ifsp[0], 0L, SEEK_END);
			SIZE = ftell(ifsp[0]);
			LEN = SIZE / sizeof(int32_t);
			rewind(ifsp[0]);
		}
		buffers[i] = (int32_t*)malloc(SIZE);

		//printf("1.0.%d.0\n", i);

		//printf("The pointer of output[%d] is: %p\n", i, output[i]);

		// Read the total data
		//printf("1.0.%d.1\n", i);

		fread(buffers[i], sizeof(int32_t), LEN, ifsp[i]);

		// Close all the input files
		fclose(ifsp[i]);
		//printf("1.0.%d.2\n", i);
	}

	for (size_t i = 0; i < col_num_out; i++) {
		output[i] = (int32_t*)malloc(SIZE);
		//printf("The pointer of output[%d] is: %p\n", i, output[i]);
	}

	//printf("1.0\n");

	// Select required rows
	size_t row_ix = 0;
	for (size_t i = 0; i < LEN; i ++) {
		//printf("%d\n", buffers[0][i]);
		int keep = 1;
		for (size_t j = 0; j < filter_num; j++) {
			int filter_cols_ix = indexof(cols[j], filter_cols);
			if (!operate(buffers[j][i], keys[filter_cols_ix], ops[filter_cols_ix])) {
				keep = 0;
				break;
			}
		}
		if (keep) {
			for (size_t j = 0; j < col_num; j ++) {
				int col_ix = indexof(cols[j], cols_out);
				if (col_ix > -1) {
					//printf("col_ix: %d\tnumber: %d\n", col_ix, buffers[j][i]);
					output[col_ix][row_ix] = buffers[j][i];
					// output_counts[row_ix] = 1;
				}
			}
			row_ix ++;
		}
	}

	//printf("1.1\n");
	//printf("%d\n", row_ix);
	size_t new_size = (row_ix + 1) * sizeof(int32_t);
	for (size_t i = 0; i < col_num; i ++) {
		free(buffers[i]);
	}
	for (size_t i = 0; i < col_num_out; i ++) {
		if (row_ix == 0) {
			free(output[i]);
			output[i] = NULL;
		} else {
			output[i] = (int32_t*)realloc(output[i], new_size);
		}
	}
	// if (new_size == 0) {
	// 	free(output_counts);
	// } else {
	// 	output_counts = (int32_t*)realloc(output_counts, new_size);
	// }


	return row_ix;
	//printf("1.2\n");
};



size_t join(size_t col_num_in_l, size_t col_num_in_r, size_t col_num_out, size_t in_len, size_t predicate_num,
	          char *cols_in_l[], char *cols_in_r[], char *cols_out[], char *filter_cols[], int cols_filter[], char ops[], int32_t consts[],
						int32_t *output[], int32_t *input[], char *path)
{
	size_t filter_len = 0;
	while (cols_filter[filter_len] != -1) {
		filter_len ++;
	};
	//printf("filter_len: %d\n", filter_len);
	FILE *ifsp[col_num_in_r], *filter_ifsp[filter_len];
	int32_t *buffers[col_num_in_r], *filter_buffers[filter_len];
	int filter_ifs_ixs[filter_len], filter_ixs[filter_len];
	//printf("length of the table_in: %d\n", LEN);
	// Initialize input file, buffers, and read all data to the memory

	//printf("%d, %d\n", col_num_in_r, col_num_out);
	for (size_t i = 0; i < col_num_in_r; i ++) {
		// open file and allocate memory for buffers
		char filename[MAX_FILE_NAME];
		add_path(path, cols_in_r[i], filename);
		//printf("%s\n", filename);
		// open file and allocate memory for buffers
		ifsp[i] = fopen(filename, "rb");
		//printf("1\n");
		buffers[i] = (int32_t*)malloc(BUFFER_SIZE);
		//printf("1.0.%d.0\n", i);
		//printf("1.0.%d.1\n", i);
	}

	for (size_t i = 0; i < filter_len; i++) {
		int filter_ix = cols_filter[i];
		filter_ixs[i] = filter_ix;
		char *filter_col_name = filter_cols[filter_ix];
		filter_ifs_ixs[i] = atoi(&filter_col_name[3]);
		char filename[MAX_FILE_NAME];
		add_path(path, filter_col_name, filename);
		//printf("filter_ix: %d\tfiltename: %s\n", filter_ix, filename);
		filter_ifsp[i] = fopen(filename, "rb");
		filter_buffers[i] = (int32_t*)malloc(BUFFER_SIZE);
	}


	for (size_t i = 0; i < col_num_out; i++) {
		output[i] = (int32_t*)malloc(BUFFER_SIZE);
		//printf("%d\n", output[i][0]);
	}

 //printf("2.0\n");

	size_t row_ix = 0;
	size_t current_max_len = INT_BUFFER_LENGTH;
	while (1) {
		int rn;
		//printf("2.0.0\n");
		for (size_t i = 0; i < col_num_in_r; i ++) {
			rn = fread(buffers[i], sizeof(int32_t), INT_BUFFER_LENGTH, ifsp[i]);
		}
		for (size_t i = 0; i < filter_len; i ++) {
			fread(filter_buffers[i], sizeof(int32_t), INT_BUFFER_LENGTH, filter_ifsp[i]);
		}
		for (size_t i = 0; i < rn; i ++) {
			int keep = 1;
			for (size_t j = 0; j < filter_len; j ++) {
				if (!operate(filter_buffers[j][i], consts[filter_ixs[j]], ops[filter_ixs[j]])) {
					keep = 0;
					break;
				}
			}
			if (keep == 0) {
				continue;
			}
			for (size_t j = 0; j < in_len; j ++) {
				keep = 1;
				//printf("Left table number: %d\tRight table number: %d\n", input[0][j], buffers[0][i]);
				for (size_t k = 0; k < predicate_num; k++) {
					if (!operate(buffers[k][i], input[k][j], '=')) {
						//printf("input[k][j]\n");
						keep = 0;
						break;
					}
				}

				if (keep) {
					for (size_t k = 0; k < col_num_in_l; k ++) {
						//printf("col: %s\n", cols_in_l[k]);
						int col_ix = indexof(cols_in_l[k], cols_out);
						//printf("col_ix: %d\n", col_ix);
						if (col_ix > -1) {
							//printf("number: %d\n", input[k][j]);
							output[col_ix][row_ix] = input[k][j];
						}
					}
					//printf("2.0.1\n");
					for (size_t k = 0; k < col_num_in_r; k ++) {
						int col_ix = indexof(cols_in_r[k], cols_out);
						if (col_ix > -1) {
							output[col_ix][row_ix] = buffers[k][i];
						}
					}
					row_ix ++;
					//printf("2.0.2\n");

					// Reallocate space
					if (row_ix == current_max_len) {
						current_max_len *= 2;
						for (size_t k = 0; k < col_num_out; k ++) {
							output[k] = realloc(output[k], current_max_len * sizeof(int32_t));
						}
					}
				}
			}
		}

		// End case
		if (rn < INT_BUFFER_LENGTH) {
      break;
    }
	}



	//printf("2.1\n");
	size_t new_size = (row_ix + 1) * sizeof(int32_t);
	for (size_t i = 0; i < col_num_in_r; i++) {
		free(buffers[i]);
		fclose(ifsp[i]);
	}
	for (size_t i = 0; i < filter_len; i++) {
		free(filter_buffers[i]);
		fclose(filter_ifsp[i]);
	}
	for (size_t i = 0; i < col_num_out; i++) {
		if (row_ix == 0) {
			free(output[i]);
			output[i] = NULL;
		} else {
			output[i] = realloc(output[i], new_size);
		}
	}
	//printf("2.2\n");

	for (size_t i = 0; i < col_num_in_l; i++) {
		free(input[i]);
		input[i] = NULL;
		//printf("free table_in[%d] address is %p\n", i, input[i]);
	}
	return row_ix;

	//printf("2.3\n");
};

void aggregate(size_t col_num, size_t in_len, int32_t *input[], FILE *ofp)
{
	//printf("len is %d\n", LEN);
	long sum[col_num];
	if (in_len == 0) {
		for (size_t i = 0; i < col_num - 1; i ++) {
			//fprintf(ofp, ",");
			printf(",");
		}
		//fprintf(ofp, "\n");
		//fflush(ofp);
		printf("\n");
		return;
	}

	for (size_t i = 0; i < col_num; i++) {
		sum[i] = 0;
	}
	for (size_t i = 0; i < in_len; i++) {
		for (size_t j = 0; j < col_num; j++) {
			sum[j] += input[j][i];
		}
	}
	//printf("3.0\n");

	// for (size_t i = 0; i < col_num; i++) {
	// 	printf("%d\n", sum[i]);
	// }
	for (size_t i = 0; i < col_num; i ++) {
		//fprintf(ofp, "%d", sum[i]);
		printf("%d", sum[i]);
		if (i == col_num - 1) {
			//fprintf(ofp, "\n");
			printf("\n");
		} else {
			fprintf(ofp, ",");
			printf(",");
		}
		//fflush(ofp);
		free((void*)input[i]);
		input[i]=NULL;
		//printf("free table_in[%d] address is %p\n", i, input[i]);
	}

	//printf("3.1\n");
	//printf("3.2\n");
};

// void transform(size_t in_col_num, size_t out_col_num, char *in_str[], char *out_str[], int32_t *input[], int32_t *output[])
// {
// 	for (size_t i = 0; i < in_col_num; i++) {
// 		int remove = 1;
// 		for (size_t j = 0; j < out_col_num; j++) {
// 			if (strcmp(in_str[i], out_str[j])) {
// 				remove = 0;
// 				output[j] = input[i];
// 				break;
// 			}
// 		}
// 	}
// }

/*
	write the table to csv
*/
// void table_write(size_t col_num, int32_t *table[])
// {
// 	FILE *ofp;
// 	//char *buffer;
//
// 	ofp = fopen("result.csv", "w");
// 	//buffer = (char*)malloc(BUFFER_SIZE);
//
// 	size_t len = sizeof(table[0]) / sizeof(int32_t);
//
// 	for (size_t i = 0; i < len; i ++) {
// 		for (size_t j = 0; j < col_num; j ++) {
// 			fprintf(ofp, "%d", table[j][i]);
// 			if (j == col_num - 1) {
// 				fprintf(ofp, "\n");
// 			} else {
// 				fprintf(ofp, ",");
// 			}
// 		}
// 	}
//
// 	// Release useless memory
// 	// for (size_t i = 0; i < col_num; i ++) {
// 	// 	free(table[i]);
// 	// }
//
// 	fclose(ofp);
// };


// int main(int argc, char *argv[])
// {
// 	clock_t start, end;
// 	double cpu_time_used;
// 	start = clock();
//
// 	char *filter_h[5], *join_ls_h[4], *join_rs_h[2], *agg_h[3];
// 	filter_h[0] = "D_c3";
// 	filter_h[1] = "D_c0";
// 	filter_h[2] = "D_c4";
// 	filter_h[3] = "D_c2";
// 	filter_h[4] = NULL;
//
// 	join_ls_h[0] = "D_c2";
// 	join_ls_h[1] = "D_c0";
// 	join_ls_h[2] = "D_c4";
// 	join_ls_h[3] = NULL;
//
// 	join_rs_h[0] = "C_c2";
// 	join_rs_h[1] = NULL;
//
// 	agg_h[0] = "D_c0";
// 	agg_h[1] = "D_c4";
// 	agg_h[2] = NULL;
//
// 	int32_t **table_in, **table_out;
// 	table_out = (int32_t**)malloc(sizeof(int32_t*) * 3);
// 	filter(4, 3, filter_h, '=', -9496, table_out, join_ls_h, argv[1]);
// 	printf("Filter Result:\n");
// 	for (size_t i = 0; i < 3; i++) {
// 		printf("%d\t", table_out[i][0]);
// 		//printf("The pointer of table_out[%d] is: %p\n", i, table_out[i]);
// 	}
// 	end = clock();
// 	cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
// 	printf("\nFilter completed, takes %.2fs\n", cpu_time_used);
// 	start = end;
//
// 	table_in = (int32_t**)malloc(sizeof(int32_t*) * 3);
// 	for (size_t i = 0; i < 3; i++) {
// 		table_in[i] = table_out[i];
// 		//printf("table_in[%d] address: %p\ttable_out[%d] address: %p\n", i, table_in[i], i, table_out[i]);
// 	}
// 	free(table_out);
// 	table_out = (int32_t**)malloc(sizeof(int32_t*) * 2);
// 	join(3, 1, 2, join_ls_h, join_rs_h, agg_h, '=', table_out, table_in, argv[1]);
// 	printf("Join Result:\n");
// 	for (size_t i = 0; i < 2; i++) {
// 		printf("%d\t", table_out[i][0]);
// 	}
// 	//printf("table_in address: %p\ttable_out address: %p\n", table_in, table_out);
// 	for (size_t i = 0; i < 3; i++) {
// 		free(table_in[i]);
// 	}
// 	free(table_in);
// 	end = clock();
// 	cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
// 	printf("\nJoin completed, takes %.2fs\n", cpu_time_used);
// 	start = end;
//
// 	aggregate(2, table_out);
// 	for (size_t i = 0; i < 2; i++) {
// 		free(table_out[i]);
// 	}
// 	free(table_out);
//
// 	end = clock();
// 	cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
// 	printf("\nAggregate complted, takes %.2fs\n", cpu_time_used);
// };
