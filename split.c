#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

const size_t BUFFER_SIZE = 4096;
const size_t INT_BUFFER_LENGTH = BUFFER_SIZE / sizeof(int32_t);

// int compare(const void * a, const void * b)
// {
//   return ( *(int32_t*)a - *(int32_t*)b );
// };
int debug = 0;

void add_path(char *path, char *elem, char *filename)
{
	strcpy(filename, path);
	strcat(filename, elem);
}

void split(char *filename)
{
  FILE *ifp;
  char *in_buffer;
  char out_file_prefix[strlen(filename)];
  strcpy(out_file_prefix, filename);
  out_file_prefix[strlen(filename) - 4] = '\0';
  ifp = fopen(filename, "r");
  in_buffer = (char*)malloc(BUFFER_SIZE);
  //setvbuf(ifp, in_buffer, _IOFBF, BUFFER_SIZE);

  //printf("Completed!\n");
  int32_t col_size = 1;
  while (1) {
    char c = fgetc(ifp);
    if (c == ',') {
      col_size ++;
    } else if (c == '\n') {
      break;
    }
  }
  //printf("%d\n", col_size);
  FILE *ofsp[col_size];
  int32_t *out_buffers[col_size];
  for (size_t i = 0; i < col_size; i++) {
    char ofn[100];
    sprintf(ofn, "%s_c%d.bin\0", out_file_prefix, i);
    ofsp[i] = fopen(ofn, "wb+");
    out_buffers[i] = (int32_t*)malloc(BUFFER_SIZE);
    //setvbuf(ofsp[i], out_buffers[i], _IOFBF, BUFFER_SIZE);
    //printf("%s is created.\n", ofn);
  };
  rewind(ifp);
  int32_t number = 0;
  int sign = 1;
  int col_i = 0;
  int row_i = 0;
  while (1) {
    int rn = fread(in_buffer, 1, BUFFER_SIZE, ifp);
    for (size_t i = 0; i < rn; i++) {
      if (in_buffer[i] == ',' || in_buffer[i] == '\n') {
        number *= sign;
        out_buffers[col_i][row_i] = number;
        //fwrite(&number, sizeof(number), 1, ofsp[col_i]);
        number = 0;
        sign = 1;
        col_i ++;
        if (in_buffer[i] == '\n') {
          col_i = 0;
          row_i ++;
          if (row_i == INT_BUFFER_LENGTH) {
            for (size_t j = 0; j < col_size; j++) {
              //qsort (out_buffers[j], row_i, sizeof(int32_t), compare);
              if (debug) {
                printf("No.%d column:\n", j);
                for (size_t k = 0; k < INT_BUFFER_LENGTH; k++) {
                  printf("%d\t", out_buffers[j][k]);
                }
                printf("\n");
              }
              fwrite(out_buffers[j], BUFFER_SIZE, 1, ofsp[j]);
            }
            row_i = 0;
          }
        }
      } else if (in_buffer[i] == '-') {
        sign = -1;
      } else {
        number = number * 10 + (in_buffer[i] - '0');
      }
    }
    if (rn < BUFFER_SIZE) {
      for (size_t j = 0; j < col_size; j++) {
        //qsort (out_buffers[j], row_i, sizeof(int32_t), compare);
        if (debug) {
          printf("No.%d column:\n", j);
          for (size_t k = 0; k < row_i; k++) {
            printf("%d\t", out_buffers[j][k]);
          }
          printf("\n");
        }
        fwrite(out_buffers[j], row_i*sizeof(int32_t), 1, ofsp[j]);
        free(out_buffers[j]);
        fclose(ofsp[j]);
      }
      break;
    }
  };

  fclose(ifp);
  free(in_buffer);
};

void split_all(char *director)
{
  DIR *d;
  struct dirent *dir;
  d = opendir(director);
  if (d)
  {
    while ((dir = readdir(d)) != NULL)
    {
      printf("%s\n", dir->d_name);
      char *filename = dir->d_name;
      size_t ix = 0;
      while (filename[ix++] != '.');
      //printf("%d\n", ix);
      if (filename[ix] != '\0') {
        char suffix[4];
        strcpy(suffix, &filename[ix]);
        printf("suffix: %s\n", suffix);
        if (strcmp(suffix, "csv") == 0) {
          char fname[25];
          add_path(director, filename, fname);
          printf("fname: %s\n", fname);
          split(fname);
        }
      }
    }
    closedir(d);
  }
}

int main(int argc, char *argv[])
{

  if (argc >= 2 && strcmp(argv[2], "--debug") == 0) {
    debug = 1;
  }
  clock_t start, end;
  double cpu_time_used;
  start = clock();
  split_all(argv[1]);
  end = clock();
  cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
  printf("Time consume: %.1fs\n", cpu_time_used);

  return 0;
}
