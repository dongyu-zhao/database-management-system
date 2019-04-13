#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

//#include "run.c"
#include "parse.c"
int debug = 0;

int main(int argc, char *argv[])
{
  clock_t start, end;
  double cpu_time_used;
  start = clock();
  char filename[MAX_FILE_NAME];
  sprintf(filename, "%s/queries.sql\0", argv[1]);
  if (argc > 3 && strcmp(argv[3], "--debug") == 0) {
    debug = 1;
  }

  //printf("%s\n", filename);
  FILE *ifp, *ofp;
  ifp = fopen(filename, "r");
  ofp = fopen(argv[2], "w");
  size_t sql_count = 0;
  while (!feof(ifp)) {
    char **agg_cols;
    table_t *tables;
    size_t agg_cols_len, tables_len;
    read_sql(ifp, agg_cols, &agg_cols_len, &tables, &tables_len);

    if (debug) {
      for (size_t i = 0; i < tables_len; i++) {
        printf("Table: %c\n", tables[i].name);
        printf("join_len: %d\tfilter_len: %d\n", tables[i].join_len, tables[i].filter_len);
        for (size_t j = 0; j < tables[i].join_len; j++) {
          printf("join_ins[%d]: %s\tjoin_outs[%d]: %s\n", j, tables[i].join_ins[j], j, tables[i].join_outs[j]);
        }
        for (size_t j = 0; j < tables[i].filter_len; j++) {
          printf("filter_cols[%d]: %s\tfilter_ops[%d]: %c\tfilter_numbers[%d]: %d\n",
                j, tables[i].filter_cols[j], j, tables[i].filter_ops[j], j, tables[i].filter_numbers[j]);
        }
        printf("\n");
      }
      printf("\n");
    }

    /*
    char *filter_h[MAX_COLS], *join_ls_h[sizes[1] / 2][MAX_COLS];
    char *join_rs_h[sizes[1] / 2 + 1][MAX_COLS], *join_out_h[sizes[1] / 2 + 1][MAX_COLS];
    size_t predicate_num[sizes[1] / 2 + 1];
    int join_filter_h[sizes[1] / 2 + 1][MAX_COLS];
    size_t filter_num = format(agg_cols, join_cols, filter_cols, sizes, ops, consts, predicate_num, filter_h, join_ls_h, join_rs_h, join_out_h, join_filter_h);
    //printf("filter_num: %d\n", filter_num);

    size_t join_size = 0;
    while (predicate_num[++join_size]);
    //printf("Join size is %d\n", join_size);

    if (debug) {
      size_t ix = 0;
      printf("filter_h: ");
      do {
        printf("%s\t", filter_h[ix]);
      } while (filter_h[++ix] != NULL);
      printf("\n\n");

      for (size_t i = 0; i < join_size; i++) {
        printf("join_ls_h[%d]: ", i);
        ix = 0;
        do {
          printf("%s\t", join_ls_h[i][ix]);
        } while(join_ls_h[i][++ix] != NULL);
        printf("\n");

        printf("join_rs_h[%d]: ", i);
        ix = 0;
        do {
          printf("%s\t", join_rs_h[i][ix]);
        } while(join_rs_h[i][++ix] != NULL);
        printf("\n");

        printf("join_out_h[%d]: ", i);
        ix = 0;
        do {
          printf("%s\t", join_out_h[i][ix]);
        } while(join_out_h[i][++ix] != NULL);
        printf("\n");

        printf("join_filter_h[%d]: ", i);
        ix = 0;
        while (join_filter_h[i][ix] != -1) {
          printf("%d\t", join_filter_h[i][ix]);
          ix ++;
        };
        printf("\n");
      }
      printf("\n");

      printf("predicate_num: ");
      for (size_t i = 0; i < join_size; i++) {
        printf("%d\t", predicate_num[i]);
      }
      printf("\n");

      printf("agg_h: ");
      for (size_t i = 0; i < sizes[0]; i++) {
        printf("%s\t", agg_cols[i]);
      }
      printf("\n\n");
    }


    int32_t **table_in, **table_out;
    size_t col_num_in_l = len(filter_h), col_num_in_r, col_num_out = len(join_ls_h[0]);
    size_t table_len;
    table_out = (int32_t**)malloc(sizeof(int32_t*) * (col_num_out + 1));

    //printf("Before Filter\n");
    table_len = filter(col_num_in_l, col_num_out, filter_h, ops, consts, filter_num, table_out, join_ls_h[0], argv[1], filter_cols);
    //printf("After Filter\n");

    if (debug == 1) {
      printf("Filter completed, there're %d rows\n", table_len);
    }
    if (debug == 2) {
      printf("Filter Result:\n");
    	for (size_t i = 0; i < table_len; i++) {
        for (size_t j = 0; j < col_num_out; j++) {
          printf("%d\t", table_out[j][i]);
        }
        printf("\n");
    	}
    }

    for (size_t i = 0; i < join_size; i++) {
      if (table_len == 0) {
        break;
      }

      char **cols_out;
      //col_num_in_l = len(join_ls_h[i]);
      //col_num_in_r = len(join_rs_h[i]);
      if (i == join_size - 1) {
        cols_out = agg_cols;
        //col_num_out = len(agg_cols);
      } else {
        cols_out = join_ls_h[i+1];
        //col_num_out = len(join_ls_h[i+1]);
      }
      col_num_in_l = len(join_ls_h[i]);
      col_num_in_r = len(join_rs_h[i]);
      col_num_out = len(cols_out);
      table_in = (int32_t**)malloc(sizeof(int32_t*) * (1 + col_num_in_l));
    	for (size_t i = 0; i < col_num_in_l; i++) {
    		table_in[i] = table_out[i];
    		//printf("table_in[%d] address: %p\ttable_out[%d] address: %p\n", i, table_in[i], i, table_out[i]);
    	}
      free(table_out);
      table_out = (int32_t**)malloc(sizeof(int32_t*) * (1 + col_num_out));

      //printf("Before join\n");
      table_len = join(col_num_in_l, col_num_in_r, col_num_out, table_len, predicate_num[i],
                       join_ls_h[i], join_rs_h[i], cols_out, filter_cols, join_filter_h[i], ops, consts,
                       table_out, table_in, argv[1]);
      //printf("Length of table is %d\n", table_len);
      //printf("After join\n");

      // for (size_t i = 0; i < col_num_in_l; i++) {
    	// 	printf("table_in[%d] address is %p\n", i, table_in[i]);
    	// }
      free(table_in);

      if (debug == 1) {
        printf("NO.%d join completed, there're %d rows\n", i + 1, table_len);
      }
      if (debug == 2) {
        printf("No.%d join result of length %d:\n", i + 1, table_len);
        for (size_t i = 0; i < table_len; i++) {
          for (size_t j = 0; j < col_num_out; j++) {
            printf("%d\t", table_out[j][i]);
          }
          printf("\n");
        }
      }
    }

    aggregate(sizes[0], table_len, table_out, ofp);
    // for (size_t i = 0; i < sizes[0]; i++) {
  	// 	printf("table_out[%d] address is %p\n", i, table_out[i]);
  	// }
  	free(table_out);

    if (debug == 1) {
      printf("Aggregate Completed\n");
    }

    */

    for (size_t i = 0; i < tables_len; i++) {
      // for (size_t j = 0; j < tables[i].join_len; j++) {
      //   free(tables[i].join_ins[j]);
      //   tables[i].join_ins[j] = NULL;
      //   free(tables[i].join_outs[j]);
      //   tables[i].join_outs[j] = NULL;
      // }
      free(tables[i].join_ins);
      free(tables[i].join_outs);
      for (size_t j = 0; j < tables[i].filter_len; j++) {
        free(tables[i].filter_cols[j]);
      }
      free(tables[i].filter_cols);
      free(tables[i].filter_ops);
      free(tables[i].filter_numbers);
    }
    printf("No.%d sql completed\n", ++ sql_count);
    fgetc(ifp); // skip '\n'

  }
  fclose(ifp);
  fclose(ofp);

  end = clock();
  cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
  printf("%.1f\n", cpu_time_used);
  return 0;
};
