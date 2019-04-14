#ifndef Table
#define Table

typedef struct table {
  char name;
  size_t join_len, filter_len;
  char **join_ins;
  char **join_outs;
  char **filter_cols;
  char *filter_ops;
  int32_t *filter_numbers;
} table_t;

#endif
