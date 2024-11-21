#include "../include/client.h"

int port = 0;

pthread_t worker_thread[100];
int worker_thread_id = 0;
char output_path[1028];

processing_args_t req_entries[100];

void * request_handle(void * img_file_path) {
  
  char out_dir_path[1028];
  //get image_file_path name and cat with output_path;
  strcpy(out_dir_path, "output/");
  strcat(out_dir_path, img_file_path);

  
  FILE* file;
  file = fopen(img_file_path, "rb");
  if (file == NULL) {
      perror("Error opening file.");
      exit(-1);
  }
  fseek(file, 0, SEEK_END);
  long int file_length = ftell(file);
  rewind(file);

  //set up connection with the port passed in through main
  int connection_fd = setup_connection(port);
  if (connection_fd == -1) {
    perror("CANNOT ESTABLISH CONNECTION");
    exit(-1);
  }


  // send_file_to_server does not have a return value
  int sentFile = send_file_to_server(connection_fd, file, file_length);
  if (sentFile == -1) {
    perror("Failed to send file.\n");
    exit(-1);
  }

  //recieve our file from the server
  int received_file = receive_file_from_server(connection_fd, out_dir_path);
  if (received_file == -1) {
    perror("Error receiving file from server.\n");
    exit(-1);
  }

  //close file
  int closed_file = fclose(file);
  if (closed_file == -1) {
    perror("Error closing file.");
  }
  return NULL;
}

/* Directory traversal function is provided to you. */
void directory_trav(char * img_directory_path) {
  char dir_path[BUFF_SIZE];
  strcpy(dir_path, img_directory_path);
  struct dirent *entry;
  DIR *dir = opendir(dir_path);
  if (dir == NULL) {
    perror("Opendir ERROR");
    exit(0);
  }
  while ((entry = readdir(dir)) != NULL) {
    if(strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && strcmp(entry->d_name, ".DS_Store") != 0) {
      sprintf(req_entries[worker_thread_id].file_name, "%s/%s", dir_path, entry->d_name);
      printf("New path: %s\n", req_entries[worker_thread_id].file_name);
      pthread_create(&worker_thread[worker_thread_id], NULL, request_handle, (void *) req_entries[worker_thread_id].file_name);
      worker_thread_id++;
    }
  }
  closedir(dir);
  for(int i = 0; i < worker_thread_id; i++) {
    pthread_join(worker_thread[i], NULL);
  }
}


int main(int argc, char *argv[]) {
  if(argc != 4) {
    fprintf(stderr, "Usage: ./client <directory path> <Server Port> <output path>\n");
    exit(-1);
  }
  
  //get our arguments and assign them accordingly
  port = atoi(argv[2]);
  strcpy(output_path, argv[3]);

  directory_trav(argv[1]);
  return 0;
}
