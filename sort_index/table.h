#ifndef Table
#define Table

typedef struct table {
  char name;
  size_t agg_len;
  size_t join_len;
  size_t filter_len;
  char **agg_cols;
  char **join_ins;
  char **join_outs;
  char **filter_cols;
  char *filter_ops;
  int32_t *filter_numbers;
} table_t;

#endif
