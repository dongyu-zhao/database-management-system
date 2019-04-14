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

int indexof(char *elem, char *ls[], size_t ls_len)
{
	for (size_t i = 0; i < ls_len; i++) {
		if (strcmp(elem, ls[i]) == 0) {
			return i;
		}
	}
	return -1;
}

// void add_path(char *path, char *elem, char *filename)
// {
// 	strcpy(filename, path);
// 	strcat(filename, elem);
// 	strcat(filename, ".bin");
// }

/*
	filter corresponding values of a table
*/
// int filter(size_t col_num, size_t col_num_out, char *cols[], char ops[], int32_t keys[],
// 					 size_t filter_num, char *cols_out[], char *path, char *filter_cols[])
// {
// 	FILE *ifsp[col_num], *ofsp[col_num_out];
// 	int32_t *buffers[col_num];
// 	int32_t *output[col_num_out];
// 	int filter_cols_ixs[filter_num];
// 	size_t SIZE, LEN;
//
// 	// Initialize input file, buffers, and read all data to the memory
// 	for (size_t i = 0; i < filter_num; i++) {
// 		filter_cols_ixs[i] = indexof(cols[i], filter_cols);
// 	}
//
// 	for (size_t i = 0; i < col_num; i ++) {
// 		char *filename[MAX_FILE_NAME];
// 		fprintf(filename, "%s/%s.bin\0", path, cols[i]);
// 		//printf("%s\n", filename);
// 		// open file and allocate memory for buffers
// 		ifsp[i] = fopen(filename, "rb");
// 		if (i == 0) {
// 			fseek(ifsp[0], 0L, SEEK_END);
// 			SIZE = ftell(ifsp[0]);
// 			LEN = SIZE / sizeof(int32_t);
// 			rewind(ifsp[0]);
// 		}
// 		buffers[i] = (int32_t*)malloc(SIZE);
//
// 		//printf("1.0.%d.0\n", i);
//
// 		//printf("The pointer of output[%d] is: %p\n", i, output[i]);
//
// 		// Read the total data
// 		//printf("1.0.%d.1\n", i);
//
// 		fread(buffers[i], sizeof(int32_t), LEN, ifsp[i]);
//
// 		// Close all the input files
// 		fclose(ifsp[i]);
// 		//printf("1.0.%d.2\n", i);
// 	}
//
// 	for (size_t i = 0; i < col_num_out; i++) {
// 		char *filename = (char*)malloc(MAX_FILE_NAME);
// 		fprintf(filename, "%s/tmp1_c%d.bin\0", path, i);
// 		ofsp[i] = fopen(filename, "wb");
// 		output[i] = (int32_t*)malloc(SIZE);
// 		cols_out[i] = filename;
// 		//printf("The pointer of output[%d] is: %p\n", i, output[i]);
// 	}
//
// 	//printf("1.0\n");
//
// 	// Select required rows
// 	size_t row_ix = 0;
// 	for (size_t i = 0; i < LEN; i ++) {
// 		//printf("%d\n", buffers[0][i]);
// 		int keep = 1;
// 		for (size_t j = 0; j < filter_num; j++) {
// 			if (!operate(buffers[j][i], keys[filter_cols_ixs[j]], ops[filter_cols_ixs[j]])) {
// 				keep = 0;
// 				break;
// 			}
// 		}
// 		if (keep) {
// 			for (size_t j = 0; j < col_num; j ++) {
// 				int col_ix = indexof(cols[j], cols_out);
// 				if (col_ix > -1) {
// 					//printf("col_ix: %d\tnumber: %d\n", col_ix, buffers[j][i]);
// 					output[col_ix][row_ix] = buffers[j][i];
// 					// output_counts[row_ix] = 1;
// 				}
// 			}
// 			row_ix ++;
// 		}
// 	}
//
// 	//printf("1.1\n");
// 	//printf("%d\n", row_ix);
// 	for (size_t i = 0; i < col_num; i ++) {
// 		free(buffers[i]);
// 	}
// 	for (size_t i = 0; i < col_num_out; i ++) {
// 		fwrite(output[i], sizeof(int32_t), len, ofsp[i]);
// 		free(output[i]);
// 	}
// 	// if (new_size == 0) {
// 	// 	free(output_counts);
// 	// } else {
// 	// 	output_counts = (int32_t*)realloc(output_counts, new_size);
// 	// }
// 	return row_ix;
// 	//printf("1.2\n");
// };



size_t join(char *cols_in_l[], size_t col_num_in_l,
						char *cols_in_r[], size_t col_num_in_r,
						char *cols_out[], size_t col_num_out,
						size_t join_num,  size_t join_ix,
						char *filter_cols[], size_t filter_len, char ops[], int32_t consts[], char *path)
{
	//printf("filter_len: %d\n", filter_len);
	FILE *ifsp_l[col_num_in_l], *ofsp[col_num_out];
	int32_t *buffers_in_l[col_num_in_l], *buffers_in_r[col_num_in_r], *filter_buffers[filter_len];
	int32_t *output[col_num_out], *table_r[col_num_in_r];
	//printf("length of the table_in: %d\n", LEN);
	// Initialize input file, buffers, and read all data to the memory

	//printf("%d, %d\n", col_num_in_r, col_num_out);
	size_t size, len;
	for (size_t i = 0; i < col_num_in_r; i ++) {
		// open file and allocate memory for buffers
		char filename[MAX_FILE_NAME];
		sprintf(filename, "%s/%s.bin\0", path, cols_in_r[i]);
		FILE *fp = fopen(filename, "rb");

		if (i == 0) {
			fseek(fp, 0L, SEEK_END);
			size = ftell(fp);
			len = size / sizeof(int32_t);
			rewind(fp);
		}
		buffers_in_r[i] = (int32_t*)malloc(size);
		table_r[i] = (int32_t*)malloc(size);
		fread(buffers_in_r[i], sizeof(int32_t), len, fp);
		fclose(fp);
	}

	for (size_t i = 0; i < filter_len; i++) {
		char filename[MAX_FILE_NAME];
		sprintf(filename, "%s/%s.bin\0", path, filter_cols[i]);
		FILE *fp = fopen(filename, "rb");
		filter_buffers[i] = (int32_t*)malloc(size);
		fread(filter_buffers[i], sizeof(int32_t), len, fp);
		fclose(fp);
	}

	size_t row_r_ix = 0;
	for (size_t i = 0; i < len; i++) {
		int keep = 1;
		for (size_t j = 0; j < filter_len; j ++) {
			if (!operate(filter_buffers[j][i], consts[j], ops[j])) {
				keep = 0;
				break;
			}
		}
		if (keep) {
			for (size_t j = 0; j < col_num_in_r; j++) {
				table_r[j][row_r_ix] = buffers_in_r[j][i];
			}
			row_r_ix ++;
		}
	}
	size_t table_r_len = row_r_ix;

	for (size_t i = 0; i < col_num_in_r; i++) {
		table_r[i] = (int32_t*)realloc(table_r[i], sizeof(int32_t) * table_r_len);
		free(buffers_in_r[i]);
	}

	for (size_t i = 0; i < filter_len; i++) {
		free(filter_buffers[i]);
	}

	for (size_t i = 0; i < col_num_out; i++) {
		char filename[MAX_FILE_NAME];
		sprintf(filename, "%s/tmp%d_c%d.bin\0", path, join_ix + 1, i);
		ofsp[i] = fopen(filename, "wb");
	}

	if (col_num_in_l == 0) {
		for (size_t i = 0; i < col_num_out; i++) {
			int col_ix = indexof(cols_out[i], cols_in_r, col_num_in_r);
			fwrite(table_r[col_ix], sizeof(int32_t), table_r_len, ofsp[i]);
			fclose(ofsp[i]);
			free(table_r[col_ix]);
		}
		return table_r_len;
	}

	for (size_t i = 0; i < col_num_in_l; i++) {
		char filename[MAX_FILE_NAME];
		sprintf(filename, "%s/tmp%d_c%d.bin\0", path, join_ix, i);
		buffers_in_l[i] = (int32_t*)malloc(BUFFER_SIZE);
		ifsp_l[i] = fopen(filename, "rb");
	}

	for (size_t i = 0; i < col_num_out; i++) {
		output[i] = (int32_t*)malloc(BUFFER_SIZE);
		//printf("%d\n", output[i][0]);
	}
	size_t row_ix = 0, row_len = 0;
	while (1) {
		int rn;
		//printf("2.0.0\n");
		for (size_t i = 0; i < col_num_in_l; i ++) {
			rn = fread(buffers_in_l[i], sizeof(int32_t), INT_BUFFER_LENGTH, ifsp_l[i]);
		}
		for (size_t i = 0; i < rn; i ++) {
			for (size_t j = 0; j < table_r_len; j ++) {
				int keep = 1;
				//printf("Left table number: %d\tRight table number: %d\n", input[0][j], buffers[0][i]);
				for (size_t k = 0; k < join_num; k++) {
					if (!operate(buffers_in_l[k][i], table_r[k][j], '=')) {
						//printf("input[k][j]\n");
						keep = 0;
						break;
					}
				}
				if (keep) {
					for (size_t k = 0; k < col_num_in_l; k ++) {
						//printf("col: %s\n", cols_in_l[k]);
						int col_ix = indexof(cols_in_l[k], cols_out, col_num_out);
						//printf("col_ix: %d\n", col_ix);
						if (col_ix > -1) {
							//printf("number: %d\n", input[k][j]);
							output[col_ix][row_ix] = buffers_in_l[k][i];
						}
					}
					//printf("2.0.1\n");
					for (size_t k = 0; k < col_num_in_r; k ++) {
						int col_ix = indexof(cols_in_r[k], cols_out, col_num_out);
						if (col_ix > -1) {
							output[col_ix][row_ix] = table_r[k][j];
						}
					}
					row_ix ++;
					row_len ++;
					//printf("2.0.2\n");

					// Reallocate space
					if (row_ix == INT_BUFFER_LENGTH) {
						for (size_t k = 0; k < col_num_out; k++) {
							fwrite(output[k], sizeof(int32_t), INT_BUFFER_LENGTH, ofsp[k]);
						}
						row_ix = 0;
					}
				}
			}
		}

		// End case
		if (rn < INT_BUFFER_LENGTH) {
			for (size_t k = 0; k < col_num_out; k++) {
				fwrite(output[k], sizeof(int32_t), row_ix, ofsp[k]);
				free(output[k]);
				fclose(ofsp[k]);
			}
      break;
    }
	}

	//printf("2.1\n");

	for (size_t i = 0; i < col_num_in_l; i++) {
		fclose(ifsp_l[i]);
		free(buffers_in_l[i]);
	}
	return row_len;
};

void aggregate(size_t col_num, size_t join_ix, FILE *ofp, char *path)
{
	//printf("len is %d\n", LEN);
	long sum[col_num];
	for (size_t i = 0; i < col_num; i++) {
		sum[i] = 0;
	}

	FILE *fps[col_num];
	int32_t *buffers[col_num];
	for (size_t i = 0; i < col_num; i ++) {
		// open file and allocate memory for buffers
		char filename[MAX_FILE_NAME];
		sprintf(filename, "%s/tmp%d_c%d.bin\0", path, join_ix, i);
		fps[i] = fopen(filename, "rb");
		buffers[i] = (int32_t*)malloc(BUFFER_SIZE);
	}

	while (1) {
		int rn;
		for (size_t i = 0; i < col_num; i++) {
			rn = fread(buffers[i], sizeof(int32_t), INT_BUFFER_LENGTH, fps[i]);
		}
		for (size_t i = 0; i < rn; i++) {
			for (size_t j = 0; j < col_num; j++) {
				sum[j] += buffers[j][i];
			}
		}

		if (rn < INT_BUFFER_LENGTH) {
			for (size_t i = 0; i < col_num; i++) {
				free(buffers[i]);
				fclose(fps[i]);
			}
			break;
		}
	}
	//printf("3.0\n");

	// for (size_t i = 0; i < col_num; i++) {
	// 	printf("%d\n", sum[i]);
	// }
	for (size_t i = 0; i < col_num; i ++) {
		fprintf(ofp, "%ld", sum[i]);
		printf("%ld", sum[i]);
		if (i == col_num - 1) {
			fprintf(ofp, "\n");
			printf("\n");
		} else {
			fprintf(ofp, ",");
			printf(",");
		}
		fflush(ofp);
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
