#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

#include "consts.h"

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

int compare(const void *a, const void *b)
{
	int *ap = *(int32_t**)a;
	int *bp = *(int32_t**)b;
	if( ap[0] > bp[0] ) return 1;
  if( ap[0] < bp[0] ) return -1;
  return 0;
}

void sort(char *filename)
{
	printf("filename: %s\n", filename);
	FILE *fp = fopen(filename, "rb+");
	int32_t *buffer;
	fseek(fp, 0L, SEEK_END);
	size_t size = ftell(fp);
	//printf("size is %d\n", size);
	size_t len = size / sizeof(int32_t);
	rewind(fp);
	// printf("length is %d\n", len);
	buffer = (int32_t*)malloc(BUFFER_SIZE);
	int32_t **data;
	data = (int32_t**)malloc(size);
	size_t ix = 0;
	while (1) {
		int rn = fread(buffer, sizeof(int32_t), INT_BUFFER_LENGTH, fp);
		for (size_t i = 0; i < rn; i++) {
			data[ix]=malloc(2 * sizeof(int32_t));
			data[ix][0] = buffer[i];
			data[ix][1] = ix;
			ix ++;
		}
		if (rn < INT_BUFFER_LENGTH) {
			break;
		}
	}
	free(buffer);
	// printf("read completed\n");
	// for (size_t i = 0; i < len; i++) {
	// 	printf("%d, %d\n", data[i][0], data[i][1]);
	// }
	// printf("Start sort\n");
	qsort(data, len, sizeof(data[0]), compare);
	// printf("End sort\n");
	// for (size_t i = 0; i < len; i++) {
	// 	printf("%d, %d\n", data[i][0], data[i][1]);
	// }
	rewind(fp);
	fwrite(data, sizeof(int32_t), len * 2, fp);
	for (size_t i = 0; i < len; i++) {
		free(data[i]);
	}
	free(data);
	fclose(fp);
}

void split_all(char *directory)
{
  DIR *d;
  struct dirent *dir;
  d = opendir(directory);
  if (d)
  {
    while ((dir = readdir(d)) != NULL)
    {

      char *filename = dir->d_name;
      size_t ix = 0;
      while (filename[ix++] != '.');
      //printf("%d\n", ix);
      if (filename[ix] != '\0') {
        char suffix[4];
        strcpy(suffix, &filename[ix]);
        //printf("suffix: %s\n", suffix);
        if (strcmp(suffix, "csv") == 0) {
          char fname[25];
          add_path(directory, filename, fname);
          //printf("fname: %s\n", fname);
          split(fname);
					printf("%s split completed\n", dir->d_name);
        }
      }
    }
    closedir(d);
  }
}

void sort_all(char *directory) {
	DIR *d;
  struct dirent *dir;
  d = opendir(directory);
  if (d)
  {
    while ((dir = readdir(d)) != NULL)
    {
      //printf("%s\n", dir->d_name);
      char *filename = dir->d_name;
      size_t ix = 0;
      while (filename[ix++] != '.');
      //printf("%d\n", ix);
      if (filename[ix] != '\0') {
        char suffix[4];
        strcpy(suffix, &filename[ix]);
        //printf("suffix: %s\n", suffix);
        if (strcmp(suffix, "bin") == 0) {
          char fname[25];
          add_path(directory, filename, fname);
          //printf("fname: %s\n", fname);
          sort(fname);
					printf("%s sort completed\n", dir->d_name);
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
  printf("Split time: %.1fs\n", cpu_time_used);
	start = end;
	sort_all(argv[1]);
	end = clock();
	cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
	printf("Sort time: %.1fs\n", cpu_time_used);
  return 0;
}
