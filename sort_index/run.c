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

int cmp(const void *a,const void *b)
{
    int32_t *ap = *(int32_t **)a;
    int32_t *bp = *(int32_t **)b;
		if (ap[0] > bp[0]) return 1;
		if (ap[0] < bp[0]) return -1;
		return 0;
}

int find_min_ix(size_t table_len, int32_t **table, int32_t x) {
	int ix = -1;
  for (int l = 0, r = table_len - 1; l <= r;) {
    int mid = l + (r - l) / 2;
    if (table[mid][0] > x) {
      r = mid - 1;
    }
    if (table[mid][0] < x) {
      l = mid + 1;
    }
    if (table[mid][0] == x) {
      ix = mid;
      r = mid - 1;
    }
  }
	return ix;
}

int find_max_ix(size_t table_len, int32_t **table, int32_t x) {
	int ix = -1;
	for (int l = 0, r = table_len - 1; l <= r;) {
	 int mid = l + (r - l) / 2;
	 if (table[mid][0] > x) {
		 r = mid - 1;
	 }
	 if (table[mid][0] < x) {
		 l = mid + 1;
	 }
	 if (table[mid][0] == x) {
		 ix = mid;
		 l = mid + 1;
	 }
	}
	return ix;
}

size_t join(char *cols_in_l[], size_t col_num_in_l,
						char *cols_in_r[], size_t col_num_in_r,
						char *cols_out[], size_t col_num_out,
						size_t join_num,  size_t join_ix,
						char *filter_cols[], size_t filter_len, char ops[], int32_t consts[], char *path)
{
	//printf("filter_len: %d\n", filter_len);
	FILE *ifsp_l[col_num_in_l], *ofsp[col_num_out];
	int32_t *buffers_in_l[col_num_in_l], *buffers_in_r[col_num_in_r], *filter_buffers[filter_len];
	int32_t *output[col_num_out], **table_r;
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
		fread(buffers_in_r[i], sizeof(int32_t), len, fp);
		fclose(fp);
	}

	table_r = (int32_t**)malloc(sizeof(int32_t*) * len);

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
			table_r[row_r_ix] = (int32_t*)malloc(sizeof(int32_t) * col_num_in_r);
			for (size_t j = 0; j < col_num_in_r; j++) {
				table_r[row_r_ix][j] = buffers_in_r[j][i];
			}
			row_r_ix ++;
		}
	}
	size_t table_r_len = row_r_ix;

	for (size_t i = 0; i < col_num_in_r; i++) {
		free(buffers_in_r[i]);
	}

	table_r = (int32_t**)realloc(table_r, sizeof(int32_t*) * table_r_len);

	for (size_t i = 0; i < filter_len; i++) {
		free(filter_buffers[i]);
	}

	for (size_t i = 0; i < col_num_out; i++) {
		char filename[MAX_FILE_NAME];
		sprintf(filename, "%s/tmp%d_c%d.bin\0", path, join_ix + 1, i);
		ofsp[i] = fopen(filename, "wb");
	}

	if (col_num_in_l == 0) {
		int col_ixs[col_num_out];
		for (size_t i = 0; i < col_num_out; i++) {
			col_ixs[i] = indexof(cols_out[i], cols_in_r, col_num_in_r);
		}

		for (size_t i = 0; i < table_r_len; i++) {
			for (size_t j = 0; j < col_num_out; j++) {
				fwrite(&table_r[i][col_ixs[j]], sizeof(int32_t), 1, ofsp[j]);
			}
			free(table_r[i]);
		}

		for (size_t i = 0; i < col_num_out; i++) {
			fclose(ofsp[i]);
		}
		return table_r_len;
	}

	qsort(table_r, table_r_len, sizeof(table_r[0]), cmp);

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
			int min_ix, max_ix;
			if (join_num == 0) {
				min_ix = 0;
				max_ix = table_r_len - 1;
			} else {
				min_ix = find_min_ix(table_r_len, table_r, buffers_in_l[0][i]);
				if (min_ix == -1) {
					continue;
				}
				max_ix = find_max_ix(table_r_len, table_r, buffers_in_l[0][i]);
			}
			for (size_t j = min_ix; j <= max_ix; j++) {
				int keep = 1;
				//printf("Left table number: %d\tRight table number: %d\n", input[0][j], buffers[0][i]);
				for (size_t k = 1; k < join_num; k++) {
					if (!operate(buffers_in_l[k][i], table_r[j][k], '=')) {
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
							output[col_ix][row_ix] = table_r[j][k];
						}
					}
					row_ix ++;
					row_len ++;

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

	for (size_t i = 0; i < table_r_len; i++) {
		free(table_r[i]);
	}
	free(table_r);

	//printf("2.1\n");

	for (size_t i = 0; i < col_num_in_l; i++) {
		fclose(ifsp_l[i]);
		free(buffers_in_l[i]);
	}
	return row_len;
};

void aggregate(size_t col_num, size_t join_ix, char *path)
{
	//printf("len is %d\n", LEN);
	int64_t sum[col_num];
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
		//fprintf(ofp, "%lld", sum[i]);
		printf("%lld", sum[i]);
		if (i == col_num - 1) {
			//fprintf(ofp, "\n");
			printf("\n");
		} else {
			//fprintf(ofp, ",");
			printf(",");
		}
		//fflush(ofp);
		//printf("free table_in[%d] address is %p\n", i, input[i]);
	}

	//printf("3.1\n");
	//printf("3.2\n");
};
