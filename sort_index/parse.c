#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "consts.h"
#include "table.h"
#include "optimizor.c"

int len(char *in[])
{
  size_t i = 0;
  while (in[i++] != NULL);
  return i - 1;
}

int read_sql(table_t **tables_p, size_t *tables_len_p, char ***agg_cols_p, size_t *agg_len)
{
  char ch;
  table_t *tables;
  size_t  ch_i;
  size_t tables_len = 0;
  size_t agg_ix = 0, tables_ix = 0, join_ix = 0, filter_ix = 0;
  char **agg_cols;

  agg_cols = (char**)malloc(sizeof(char*) * MAX_COLS);
  tables = (table_t*)malloc(sizeof(table_t) * MAX_TABLES);

  char str[6];
  do {
    ch = getchar();
    switch (ch) {
      case '(': ch_i = 0; break;
      case ')': {
        str[ch_i] = '\0';
        char *new_str;
        new_str = (char*)malloc(6);
        strcpy(new_str, str);
        agg_cols[agg_ix ++] = new_str;
        break;
      }
      case '.': str[ch_i++] = '_'; break;
      default : if (ch_i < 6) str[ch_i++] = ch;
    };
  } while (ch != '\n');
  agg_cols = (char**)realloc(agg_cols, sizeof(char*) * agg_ix);
  *agg_cols_p = agg_cols;
  *agg_len = agg_ix;
  //printf("1\n");

  do {
    ch = getchar();
    switch (ch) {
      case '\n': break;
      case ',': tables_ix ++; break;
      default : tables[tables_ix].name = ch;
    }
  } while (ch != '\n') ;
  tables_len = tables_ix + 1;
  tables = (table_t*)realloc(tables, sizeof(table_t) * tables_len);
  *tables_len_p = tables_len;

  char **aggs_cols[tables_len], **joins_ins[tables_len], **joins_outs[tables_len];
  char **filters_cols[tables_len];
  char *filters_ops[tables_len];
  int32_t *filters_numbers[tables_len];
  size_t aggs_ix[tables_len], joins_ix[tables_len], filters_ix[tables_len];
  for (size_t i = 0; i < tables_len; i++) {
    aggs_cols[i] = (char**)malloc(sizeof(char*) * MAX_COLS);
    joins_ins[i] = (char**)malloc(sizeof(char*) * MAX_COLS);
    joins_outs[i] = (char**)malloc(sizeof(char*) * MAX_COLS);
    filters_cols[i] = (char**)malloc(sizeof(char*) * MAX_COLS);
    filters_ops[i] = (char*)malloc(MAX_COLS);
    filters_numbers[i] = (int32_t*)malloc(sizeof(int32_t) * MAX_COLS);
    aggs_ix[i] = 0;
    joins_ix[i] = 0;
    filters_ix[i] = 0;
  }
  //printf("2\n");

  for (size_t i = 0; i < agg_ix; i++) {
    //printf("agg_cols[%d] is %s\n", i, agg_cols[i]);
    int table_ix = table_index_of(agg_cols[i][0], tables, tables_len);
    aggs_cols[table_ix][aggs_ix[table_ix] ++] = agg_cols[i];
    //printf("aggs_ix[%d] is %d\n", table_ix, aggs_ix[table_ix]);
  }

  char *join_cols[MAX_COLS];
  int is_start = 1;
  do {
    ch = getchar();
    switch (ch) {
      case '\n':
      case ' ': if (is_start) {
        ch_i = 0;
        is_start = 0;
      } else {
        str[ch_i] = '\0';
        char *new_str;
        new_str = (char*)malloc(6);
        strcpy(new_str, str);
        //printf("%s\n", new_str);
        join_cols[join_ix ++] = new_str;
        is_start = 1;
      }; break;
      case '.': str[ch_i++] = '_'; break;
      default : if (ch_i < 6) str[ch_i++] = ch;
    };
  } while (ch != '\n');
  //printf("3\n");
  for (size_t i = 0; i < join_ix; i+=2) {
    int table_ix_1 = table_index_of(join_cols[i][0], tables, tables_len);
    int table_ix_2 = table_index_of(join_cols[i+1][0], tables, tables_len);
    joins_ins[table_ix_1][joins_ix[table_ix_1]] = join_cols[i];
    joins_ins[table_ix_2][joins_ix[table_ix_2]] = join_cols[i+1];
    joins_outs[table_ix_1][joins_ix[table_ix_1] ++] = join_cols[i+1];
    joins_outs[table_ix_2][joins_ix[table_ix_2] ++] = join_cols[i];
  }

  while (ch != ';') {
    do {ch = getchar();} while (ch != ' ');
    ch_i = 0;
    do {
      ch = getchar();
      str[ch_i ++] = ch;
    } while (ch != ' ');
    str[--ch_i] = '\0';
    char *new_str;
    new_str = (char*)malloc(6);
    strcpy(new_str, str);
    new_str[1] = '_';
    int table_ix = table_index_of(new_str[0], tables, tables_len);
    filters_cols[table_ix][filters_ix[table_ix]] = new_str;
    //printf("%s\n", new_str);
    ch = getchar();
    filters_ops[table_ix][filters_ix[table_ix]] = ch;
    //printf("%c\n", ops[sizes[2]]);
    getchar(); // skip ' '
    int32_t number = 0;
    int sign = 1;
    while (1) {
      ch = getchar();
      if (ch == ' ' || ch == ';') {
        break;
      } else if (ch == '-') {
        sign = -1;
      } else {
        number = number * 10 + (ch - '0');
      }
    };
    filters_numbers[table_ix][filters_ix[table_ix] ++] = number * sign;
  }

  for (size_t i = 0; i < tables_len; i++) {
    aggs_cols[i] = (char**)realloc(aggs_cols[i], sizeof(char*) * aggs_ix[i]);
    joins_ins[i] = (char**)realloc(joins_ins[i], sizeof(char*) * joins_ix[i]);
    joins_outs[i] = (char**)realloc(joins_outs[i], sizeof(char*) * joins_ix[i]);
    filters_cols[i] = (char**)realloc(filters_cols[i], sizeof(char*) * filters_ix[i]);
    filters_ops[i] = (char*)realloc(filters_ops[i], filters_ix[i]);
    filters_numbers[i] = (int32_t*)realloc(filters_numbers[i], sizeof(int32_t) * filters_ix[i]);
  }

  for (size_t i = 0; i < tables_len; i++) {
    tables[i].agg_len = aggs_ix[i];
    tables[i].join_len = joins_ix[i];
    tables[i].filter_len = filters_ix[i];
    tables[i].agg_cols = aggs_cols[i];
    tables[i].join_ins = joins_ins[i];
    tables[i].join_outs = joins_outs[i];
    tables[i].filter_cols = filters_cols[i];
    tables[i].filter_ops = filters_ops[i];
    tables[i].filter_numbers = filters_numbers[i];
  }
  *tables_p = tables;
  //printf("4\n");

  //skip "\n\n"
  getchar();
  getchar();
};


int not_in(char *elem, char *set[], size_t size)
{
  for (size_t i = 0; i < size; i++) {
    if (!strcmp(elem, set[i])) {
      return 0;
    }
  }
  return 1;
}

void append_table(size_t sizes[], size_t start, char table, char *out[], char *agg_cols[], char *join_cols[])
{
  size_t ix = start;
  //printf("out[0]:%s\n", out[0]);
  for (size_t i = 0; i < sizes[0]; i++) {
    //printf("%s, %c, %d\n", agg_cols[i], table, not_in(agg_cols[i], out, ix));
    //printf("%d\n", not_in(agg_cols[i], out, ix));
    if (agg_cols[i][0] == table && not_in(agg_cols[i], out, ix)) {
      out[ix ++] = agg_cols[i];
      //printf("out[%d] = %s\n", ix - 1, out[ix - 1]);
    }
  }
  for (size_t i = 0; i < sizes[1]; i++) {
    if (join_cols[i] != NULL && join_cols[i][0] == table && not_in(join_cols[i], out, ix)) {
      out[ix ++] = join_cols[i];
      //printf("out[%d] = %s\n", ix - 1, out[ix - 1]);
    }
  }
  out[ix] = NULL;
}

void append_cols(size_t sizes[], size_t start, char *in[], char *out[], char *agg_cols[], char *join_cols[])
{
  size_t ix = start, i = 0;
  while (in[i] != NULL) {
    for (size_t j = 0; j < sizes[0]; j++) {
      if (!strcmp(agg_cols[j], in[i]) && not_in(in[i], out, ix)) {
        out[ix ++] = agg_cols[j];
        //printf("out%d: %s\n", ix, out[ix - 1]);
        break;
      }
    }
    for (size_t j = 0; j < sizes[1]; j++) {
      if (join_cols[j] != NULL && !strcmp(join_cols[j], in[i]) && not_in(in[i], out, ix)) {
        out[ix ++] = join_cols[j];
        //printf("out%d: %s\n", ix, out[ix - 1]);
        break;
      }
    }
    i ++;
  }
  out[ix] = NULL;
}

int table_in(char table, char *set[], size_t size)
{
  for (size_t i = 0; i < size; i++) {
    if (set[i] != NULL && table == set[i][0]) {
      return 1;
    }
  }
  return 0;
}

size_t simplify(size_t sizes[], char *in[], char *out_l[], char *out_r[],
                int out_filter[], char *agg_cols[], char *join_cols[], char *filter_cols[])
{
  char table_l, table_r = '\0';
  size_t in_len = len(in);
  size_t i = 0, ix = 0;
  for (size_t j = 0; j < sizes[2]; j++) {
    if (!table_in(filter_cols[j][0], in, in_len) && table_in(filter_cols[j][0], join_cols, sizes[1])) {
      table_r = filter_cols[j][0];
    }
  }
  while (in[i] != NULL) {
    table_l = in[i][0];
    //printf("%c\n", table_l);
    for (size_t j = 0; j < sizes[1]; j++) {
      if (join_cols[j] != NULL && join_cols[j][0] == table_l) {

        if (j % 2 && (ix == 0 || table_r == join_cols[j - 1][0])) {
          out_r[ix] = join_cols[j - 1];
          join_cols[j - 1] = NULL;
        } else if (j % 2 == 0 && (ix == 0 || table_r == join_cols[j + 1][0])) {
          out_r[ix] = join_cols[j + 1];
          join_cols[j + 1] = NULL;
        } else {
          continue;
        }
        out_l[ix++] = join_cols[j];
        table_r = out_r[0][0];
        join_cols[j] = NULL;
        //printf("%s\n", out_l[ix - 1]);
        //printf("%s\n", out_r[ix - 1]);
      }
    }
    i ++;
  }
  //printf("out_l[0] = %s\n", out_l[0]);
  //printf("out_r[0] = %s\n", out_r[0]);

  //printf("0.1\n");
  append_cols(sizes, ix, in, out_l, agg_cols, join_cols);
  //printf("0.2\n");
  append_table(sizes, ix, table_r, out_r, agg_cols, join_cols);
  //printf("0.3\n");
  size_t filter_ix = 0;
  for (size_t i = 0; i < sizes[2]; i++) {
    if (filter_cols[i][0] == table_r) {
      //printf("index of filter_cols: %d\n", i);
      out_filter[filter_ix ++] = i;
    }
  }
  // for (size_t i = 0; i < filter_ix; i++) {
  //   printf("%d\t", out_filter[i]);
  // }
  // printf("\n");
  out_filter[filter_ix] = -1;

  // filter_ix = 0;
  // printf("filter_h:\n");
  // do {
  //   printf("%s\t", in[filter_ix]);
  // } while (in[++filter_ix] != NULL);
  // printf("\n");
  return ix;
}



void join_h(char *l[], char *r[], char *out[])
{
  size_t i = 0, j = 0;
  do {
    out[j ++] = l[i];
  } while(l[++i] != NULL);
  i = 0;
  do {
    out[j ++] = r[i];
  } while(r[++i] != NULL);
  out[j] = NULL;
}

int all_null(size_t d, char *in[]) {
  for (size_t i = 0; i < d; i++) {
    if (in[i] != NULL) {
      return 0;
    }
  }
  return 1;
}

void format(table_t tables[], size_t tables_len, char **joins_l_h[],  size_t joins_l_len[],
            char **joins_r_h[], size_t joins_r_len[], size_t joins_num[])
{
  for (size_t i = 0; i < tables_len; i++) {
    table_t table_r = tables[i];
    size_t ix_l = 0, ix_r = 0;
    joins_l_h[i] = (char**)malloc(sizeof(char*) * MAX_COLS);
    joins_r_h[i] = (char**)malloc(sizeof(char*) * MAX_COLS);
    for (size_t j = 0; j < table_r.join_len; j++) {
      int table_ix = table_index_of(table_r.join_outs[j][0], tables, i);
      if (table_ix >= 0) {
        joins_l_h[i][ix_l ++] = table_r.join_outs[j];
        joins_r_h[i][ix_r ++] = table_r.join_ins[j];
      }
    }
    joins_num[i] = ix_l;

    for (size_t j = 0; j < i; j++) {
      table_t table_l = tables[j];
      for (size_t k = 0; k < table_l.agg_len; k++) {
        if (not_in(table_l.agg_cols[k], joins_l_h[i], ix_l)) {
          joins_l_h[i][ix_l ++] = table_l.agg_cols[k];
        }
      }
      for (size_t k = 0; k < table_l.join_len; k++) {
        if (not_in(table_l.join_ins[k], joins_l_h[i], ix_l)) {
          int table_ix = table_index_of(table_l.join_outs[k][0], tables, i);
          if (table_ix == -1) {
            joins_l_h[i][ix_l ++] = table_l.join_ins[k];
          }
        }
      }
    }

    for (size_t j = 0; j < table_r.agg_len; j++) {
      if (not_in(table_r.agg_cols[j], joins_r_h[i], ix_r)) {
        joins_r_h[i][ix_r ++] = table_r.agg_cols[j];
      }
    }
    for (size_t j = 0; j < table_r.join_len; j++) {
      if (not_in(table_r.join_ins[j], joins_r_h[i], ix_r)) {
        joins_r_h[i][ix_r ++] = table_r.join_ins[j];
      }
    }
    joins_l_h[i] = (char**)realloc(joins_l_h[i], sizeof(char*) * ix_l);
    joins_r_h[i] = (char**)realloc(joins_r_h[i], sizeof(char*) * ix_r);
    joins_l_len[i] = ix_l;
    joins_r_len[i] = ix_r;
  }
  // filter_ix = 0;
  // printf("filter_h:\n");
  // do {
  //   printf("%s\t", filter_h[filter_ix]);
  // } while (filter_h[++filter_ix] != NULL);
  // printf("\n");
};

// int main(int argc, char *argv[])
// {
//   clock_t start, end;
//   double cpu_time_used;
//   start = clock();
//
//   FILE *ifp;
//   ifp = fopen(argv[1], "r");
//
//   // size_t count = 0;
//   // while(1) {
//   //   char ch = fgetc(ifp);
//   //   if (ch == '\n') {
//   //     break;
//   //   } else {
//   //     count = count * 10 + (ch - '0');
//   //   }
//   // };
//   // printf("%d\n", count);
//
//   size_t max_size = 10;
//   size_t max_ch = 6;
//   while (!feof(ifp)) {
//     char *agg_cols[max_size], *join_cols[max_size], *filter_cols[max_size], ops[max_size];
//     size_t sizes[3];
//     int32_t consts[max_size];
//     read_sql(ifp, agg_cols, join_cols, filter_cols, sizes, ops, consts);
//
//     for (size_t j = 0; j < sizes[0]; j++) {
//       printf("%s\t", agg_cols[j]);
//     }
//     printf("\n");
//     for (size_t j = 0; j < sizes[1]; j++) {
//       printf("%s\t", join_cols[j]);
//     }
//     printf("\n");
//     for (size_t j = 0; j < sizes[2]; j++) {
//       printf("%s %c %d\t", filter_cols[j], ops[j], consts[j]);
//     }
//     printf("\n\n");
//
//     char *filter_h[MAX_COLS], *join_ls_h[sizes[1] / 2][max_size];
//     char *join_rs_h[sizes[1] / 2 + 1][max_size], *join_out_h[sizes[1] / 2 + 1][max_size];
//     char *join_filter_h[sizes[1] / 2 + 1][max_size];
//     size_t predicate_num[MAX_COLS];
//
//     size_t filter_num = format(agg_cols, join_cols, filter_cols, sizes, ops, consts, predicate_num, filter_h, join_ls_h, join_rs_h, join_out_h);
//     printf("%d\n", filter_num);
//     size_t ix = 0;
//
//     printf("filter_h: ");
//     do {
//       printf("%s\t", filter_h[ix]);
//     } while (filter_h[++ix] != NULL);
//     printf("\n\n");
//
//     size_t join_size = 0;
//     while (predicate_num[++join_size]);
//     for (size_t i = 0; i < join_size; i++) {
//       printf("join_ls_h[%d]: ", i);
//       ix = 0;
//       do {
//         printf("%s\t", join_ls_h[i][ix]);
//       } while(join_ls_h[i][++ix] != NULL);
//       printf("\n");
//
//       printf("join_rs_h[%d]: ", i);
//       ix = 0;
//       do {
//         printf("%s\t", join_rs_h[i][ix]);
//       } while(join_rs_h[i][++ix] != NULL);
//       printf("\n");
//
//       printf("join_out_h[%d]: ", i);
//       ix = 0;
//       do {
//         printf("%s\t", join_out_h[i][ix]);
//       } while(join_out_h[i][++ix] != NULL);
//       printf("\n");
//     }
//     printf("\n");
//
//     printf("predicate_num: ");
//     for (size_t i = 0; i < sizes[1] / 2; i++) {
//       if (predicate_num[i]) {
//         printf("%d\t", predicate_num[i]);
//       } else {
//         break;
//       }
//     }
//     printf("\n");
//
//     printf("agg_h: ");
//     for (size_t i = 0; i < sizes[0]; i++) {
//       printf("%s\t", agg_cols[i]);
//     }
//     printf("\n\n");
//     fgetc(ifp); // skip '\n'
//   }
//
//   end = clock();
//   cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
//   printf("%.1f\n", cpu_time_used);
//   return 0;
// };

//
// int filter(char *table, int32_t key, int d, char *vales[])
// {
//   FILE *ifp, *ofp;
//   int32_t *key_buffer, *out_buffer, *in_buffers[d];
//   const size_t ROW_SIZE = INT_BUFFER_LENGTH - INT_BUFFER_LENGTH % d;
//   ifp = fopen(fidame, "rb");
//   key_buffer = (int32_t*)malloc(BUFFER_SIZE);
//   for (size_t i = 0; i < d; i++) {
//     ifsp = fopen(values[i], "rb");
//     in_buffers[i] = (int32_t*)malloc(BUFFER_SIZE);
//   }
//   ofp = fopen("tmp.tbl", "wb");
//   fwrite(&d, sizeof(int), 1, ofp);
//   out_buffer = (int32_t*)malloc(ROW_SIZE * sizeof(int32_t));
//   while (1) {
//     int rn = fread(key_buffer, sizeof(int32_t), INT_BUFFER_LENGTH, ifp);
//     for (size_t i = 0; i < d; i++) {
//       fread(in_buffers[i], sizeof(int32_t), INT_BUFFER_LENGTH, ifsp);
//     }
//     size_t row_i = 0;
//
//     for (size_t i = 0; i < rn; i++) {
//       if (key_buffer[i] == key) {
//         for (size_t j = 0; j < d; j++) {
//           out_buffer[row_i++] = in_buffers[j][i];
//         }
//       }
//       if (row_i == ROW_SIZE) {
//         fwrite(out_buffer, sizeof(out_buffer), 1, ofp);
//         row_i = 0;
//       }
//     }
//
//     if (rn < INT_BUFFER_LENGTH) {
//       free(key_buffer);
//       fclose(ifp)
//       fwrite(out_buffer, row_i*sizeof(int32_t), 1, ofp);
//       free(out_buffer);
//       fclose(ofp);
//       break;
//     }
//   }
//
// };
