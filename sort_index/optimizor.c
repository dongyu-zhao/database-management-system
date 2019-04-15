#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "table.h"

size_t log2_estimate(int32_t x) {
  size_t log2 = 0;
  while (x > 0) {
    x >>= 1;
    log2++;
  }
  return log2;
}

int table_index_of(char table, table_t *tables, size_t tables_len)
{
  for (size_t i = 0; i < tables_len; i++) {
    if (tables[i].name == table) {
      return i;
    }
  }
  return -1;
}

size_t col_index_of(char* col) {
  size_t ix = 0;
  size_t char_ix = 3;
  while (col[char_ix] != '\0') {
    ix = ix * 10 + col[char_ix] - '0';
    char_ix ++;
  }
  return ix;
}

size_t unique_estimate(int32_t max, int32_t min)
{
  if (max >= 0 && min >= 0 || max <= 0 && min <= 0) {
    return 1;
  }
  return (log2_estimate(max) + log2_estimate(-min)) / 2;
}

// size_t read_cost(table_t table, char *path)
// {
//   size_t size_log = log2_estimate(rows_len);
//   if (table.filter_len == 0) {
//     return size_log;
//   }
//   char fname[MAX_FILE_NAME];
//   sprintf(fname, "%s/%c_h.bin", path, table.name);
//   FILE *fp = fopen(fname, "rb");
//   size_t rows_len, cols_len;
//   int32_t maxs[cols_len], mins[cols_len];
//   fread(&rows_len, sizeof(size_t), 1, fp);
//   fread(&cols_len, sizeof(size_t), 1, fp);
//   fread(maxs, sizeof(int32_t), cols_len, fp);
//   fread(mins, sizeof(int32_t), cols_len, fp);
//   fclose(fp);
//
//   for (size_t i = 0; i < table.filter_len; i++) {
//     int ix = col_index_of(table.filter_cols[i]);
//     if (table.filter_ops[i] == '=') {
//       size_log -= unique_estimate(maxs[ix], mins[ix]);
//     } else if (table.filter_ops[i] == '>'){
//       size_log -= log2_estimate((maxs[ix] - mins[ix]) / (maxs[ix] - table.filter_numbers[i]));
//     } else {
//       size_log -= log2_estimate((maxs[ix] - mins[ix]) / (table.filter_numbers[i] - mins[ix]));
//     }
//     if (size_log <= 0) {
//       return 1;
//     }
//   }
//   return size_log;
// }

size_t size_max(size_t x, size_t y) {
  if (x < y) return y;
}

int cost(table_t table_ls[], size_t table_ls_len, table_t table_r, size_t size_l_log)
{

  char fname_r[MAX_FILE_NAME];
  sprintf(fname_r, "%c_h.bin", table_r.name);
  FILE *fp_r = fopen(fname_r, "rb");
  size_t rows_len_r, cols_len_r;
  fread(&rows_len_r, sizeof(size_t), 1, fp_r);
  fread(&cols_len_r, sizeof(size_t), 1, fp_r);
  int32_t maxs_r[cols_len_r], mins_r[cols_len_r];
  fread(maxs_r, sizeof(int32_t), cols_len_r, fp_r);
  fread(mins_r, sizeof(int32_t), cols_len_r, fp_r);
  fclose(fp_r);

  size_t size_log = log2_estimate(rows_len_r);

  for (size_t i = 0; i < table_r.filter_len; i++) {
    int ix_filter = col_index_of(table_r.filter_cols[i]);
    size_t diff;
    if (table_r.filter_ops[i] == '=') {
      diff = unique_estimate(maxs_r[ix_filter], mins_r[ix_filter]);
    } else if (table_r.filter_ops[i] == '>'){
      diff = log2_estimate((maxs_r[ix_filter] - mins_r[ix_filter]) / (maxs_r[ix_filter] - table_r.filter_numbers[i]));
    } else {
      diff = log2_estimate((maxs_r[ix_filter] - mins_r[ix_filter]) / (table_r.filter_numbers[i] - mins_r[ix_filter]));
    }
    if (size_log <= diff) {
      size_log = 1;
      break;
    }
    size_log -= diff;
  }

  if (table_ls_len == 0) {
    return size_log;
  }

  int has_join = 0;
  for (size_t i = 0; i < table_r.join_len; i++) {
    if (table_index_of(table_r.join_outs[i][0], table_ls, table_ls_len) > -1) {
      has_join = 1;
      break;
    }
  }
  if (!has_join) {
    return 32+size_l_log;
  }
  size_log += size_l_log;

  for (size_t i = 0; i < table_r.join_len; i++) {
    int ix = table_index_of(table_r.join_outs[i][0], table_ls, table_ls_len);
    if (ix >= 0) {
      char fname_l[MAX_FILE_NAME];
      sprintf(fname_l, "%c_h.bin", table_ls[ix].name);
      FILE *fp_l = fopen(fname_l, "rb");
      size_t rows_len_l, cols_len_l;
      fread(&rows_len_l, sizeof(size_t), 1, fp_l);
      fread(&cols_len_l, sizeof(size_t), 1, fp_l);
      int32_t maxs_l[cols_len_l], mins_l[cols_len_l];
      fread(maxs_l, sizeof(int32_t), cols_len_l, fp_l);
      fread(mins_l, sizeof(int32_t), cols_len_l, fp_l);
      fclose(fp_l);
      size_t ix_r = col_index_of(table_r.join_ins[i]);
      size_t ix_l = col_index_of(table_r.join_outs[i]);
      size_t unique_l = unique_estimate(maxs_l[ix_l], mins_l[ix_l]) + log2_estimate(rows_len_l) - size_l_log;
      if (unique_l < size_l_log) {
        unique_l = size_l_log;
      }
      size_log -= log2_estimate(size_max(unique_l, unique_estimate(maxs_r[ix_r], mins_r[ix_r])));
      if (size_log <= 0) {
        return 1;
      }
    }
  }
  return size_log;
}

size_t size_two_pow(size_t x) {
  size_t y = 1;
  while (x > 0) {
    y <<= 1;
    x --;
  }
  return y;
}

size_t compute_best(size_t ix, size_t best[], table_t tables[], size_t tables_len, size_t *tables_ix) {
  if (tables_len == 0) {
    return 0;
  }
  if (best[ix] != 0) return best[ix];

  table_t tables_copy[tables_len];
  for (size_t i = 0; i < tables_len; i++) {
    tables_copy[i] = tables[i];
  }
  size_t size_curr_log = (size_t)MAX_VALUE;
  for (size_t i = 0; i < tables_len; i++) {
    // internal->size = rels.size - 1;
    // internal->elems = (char*)malloc(internal.size + 1);
    table_t sub_tables[tables_len - 1];
    // strncpy(internal.elems, rels.elems, i);
    for (size_t j = 0; j < i; j++) {
      sub_tables[j] = tables_copy[j];
    }
    // strcpy(&internal.elems[i], &rels.elems[i+1]);
    for (size_t j = i + 1; j < tables_len; j ++) {
      sub_tables[j - 1] = tables_copy[j];
    }
    size_t size_l_log = compute_best(ix - tables_ix[tables_copy[i].name - 'A'], best, sub_tables, tables_len - 1, tables_ix);
    size_t size_log = size_l_log + cost(sub_tables, tables_len - 1, tables_copy[i], size_l_log);
    if (size_log < size_curr_log) {
      size_curr_log = size_log;
      for (size_t j = 0; j < tables_len - 1; j++) {
        tables[j] = sub_tables[j];
      }
      tables[tables_len - 1] = tables_copy[i];
    }
  }
  best[ix] = size_curr_log;
}

void table_sort(table_t tables[], size_t tables_len) {
  size_t len = size_two_pow(tables_len);
  size_t best[len];
  for (size_t i = 0; i < len; i++) {
    best[i] = 0;
  }
  size_t tables_ix[26];
  int ix = 1;
  for (size_t i = 0; i < tables_len; i++) {
    tables_ix[tables[i].name - 'A'] = ix;
    ix *= 2;
  }
  compute_best(len - 1, best, tables, tables_len, tables_ix);
  // for (size_t i = 1; i < len; i++) {
  //   printf("best[%d] is :%d\n", i, best[i]);
  // }
}

void table_sort2(table_t tables[], size_t tables_len) {
  table_t tables_copy[tables_len];
  for (size_t i = 0; i < tables_len; i++) {
    tables_copy[i] = tables[i];
  }
  size_t table_ix = 0;
  for (size_t i = 0; i < tables_len; i++) {
    if (tables_copy[i].filter_len > 0) {
      tables[table_ix++] = tables_copy[i];
      break;
    }
  }
  while (table_ix < tables_len) {
    for (size_t i = 0; i < tables_len; i++) {
      table_t table = tables_copy[i];
      if (table_index_of(table.name, tables, table_ix) > -1) {
        continue;
      }
      for (size_t j = 0; j < table.join_len; j++) {
        if (table_index_of(table.join_outs[j][0], tables, table_ix) > -1) {
          tables[table_ix ++] = tables_copy[i];
          break;
        }
      }
    }
  }
}
