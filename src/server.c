#include "../include/server.h"
#include <dirent.h>

// /********************* [ Helpful Global Variables ] **********************/
int num_dispatcher = 0;  // Global integer for the number of dispatchers.
int num_worker = 0;      // Global integer for the number of worker threads.
FILE *logfile;           // Global file pointer to the log file.
int queue_len = 0;       // Global integer for the length of the queue.

#define WORKER_DISPATCHER 100

// Keep track of worker and dispatcher threads and their ids
pthread_t worker_thread[MAX_THREADS];
pthread_t dispatcher_thread[MAX_THREADS];
int thread_ids[MAX_THREADS*2];

// Our locks and CVs for the lock when swapping between worker and dispatcher
pthread_mutex_t queue_access = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_full = PTHREAD_COND_INITIALIZER;
pthread_cond_t queue_not_empty = PTHREAD_COND_INITIALIZER;

// The database that will hold the images and keep track of how many entries
database_entry_t database[100];
int num_data_entries = 0;

// Queue that holds our thread requests
request_t *queue[MAX_QUEUE_LEN];

// Request index
int req_index = 0;
int queue_position = 0;

// Size of our queue
int queue_size = 0;

/**********************************************
 * image_match
   - parameters:
      - input_image is the image data to compare
      - size is the size of the image data
   - returns:
       - database_entry_t that is the closest match to the input_image
************************************************/
database_entry_t image_match(char *input_image, int size) {
  int closest_distance = 10;
  int closest_index = -1;
  const char* closest_file = NULL;

  for (int i = 0; i < num_data_entries; i++) {
    const char *db_image = database[i].buffer;
    int result = abs(memcmp(input_image, db_image, size));

    if (result == 0) {
      return database[i];
    } else if (result < closest_distance) {
      closest_distance = result;
      closest_index = i;
      closest_file = db_image;
    }
  }

  if (closest_file != NULL) {
    return database[closest_index];
  }

  printf("No closest file found.\n");
}

/**********************************************
 * LogPrettyPrint
   - parameters:
      - to_write is expected to be an open file pointer, or it
        can be NULL which means that the output is printed to the terminal
      - All other inputs are self explanatory or specified in the writeup
   - returns:
       - no return value
************************************************/
void LogPrettyPrint(
  FILE* to_write, int threadId, int requestNumber,
  char* file_name, int file_size) {
  if (to_write == NULL) {
// print out to stdout
    printf("****** Server Log Begin ******\n");
    printf("Thread ID: %d\n", threadId);
    printf("Request Number: %d\n", requestNumber);
    printf("File Name: %s\n", file_name);
    printf("File Size: %d\n", file_size);
    printf("****** Server Log End ******\n");
    printf("\n");
} else {
    fprintf(to_write, "****** Server Log Begin ******\n");
    fprintf(to_write, "Thread ID: %d\n", threadId);
    fprintf(to_write, "Request Number: %d\n", requestNumber);
    fprintf(to_write, "File Name: %s\n", file_name);
    fprintf(to_write, "File Size: %d\n", file_size);
    fprintf(to_write, "****** Server Log End ******\n");
    fprintf(to_write, "\n");
  }
}

// Provided by course staff
void loadDatabase(char *path) {
  struct dirent *entry;
  DIR *dir = opendir(path);
  if (dir == NULL) {
    perror("Opendir ERROR");
    exit(0);
  }
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") != 0 &&
        strcmp(entry->d_name, "..") != 0 &&
        strcmp(entry->d_name, ".DS_Store") != 0) {
      sprintf(
        database[num_data_entries].file_name, "%s/%s", path, entry->d_name);
      FILE *fp1;
      unsigned char *buffer1 = NULL;
      long fileLength1;

      // Open the image files
      fp1 = fopen(database[num_data_entries].file_name, "rb");
      if (fp1 == NULL) {
        perror("Error: Unable to open image files.\n");
        exit(1);
      }

      // Get the length of file 1
      fseek(fp1, 0, SEEK_END);
      fileLength1 = ftell(fp1);
      rewind(fp1);

      // Allocate memory to store file contents
      buffer1 = (unsigned char*) malloc(fileLength1 * sizeof(unsigned char));

      if (buffer1 == NULL) {
        printf("Error: Memory allocation failed.\n");
        fclose(fp1);
        if (buffer1 != NULL) free(buffer1);
      }

      // Read file contents into memory buffers
      fread(buffer1, sizeof(unsigned char), fileLength1, fp1);
      database[num_data_entries].buffer = buffer1;
      database[num_data_entries].file_size = fileLength1;
      num_data_entries++;
    }
  }
  closedir(dir);
}

void * dispatch(void *thread_id) {
  while (1) {
    size_t file_size = 0;
    request_detials_t request_details;

    int fd = accept_connection();
    printf("Dispatcher accepted connection\n");

    char *buffer = get_request_server(fd, &file_size);
    if (!buffer || file_size == 0) {
      perror("Failed to get request");
      close(fd);
      continue;
    }

    /* Copy the filename from get_request_server
       into allocated memory, to put on request queue */
    request_t *cur_request = malloc(sizeof(request_t));

    // Need to initialize buffer since it's a pointer to an array in the struct
    cur_request->buffer = malloc(file_size);

    // Copy the contents to the request
    memcpy(cur_request->buffer, buffer, file_size);
    cur_request->file_size = file_size;
    cur_request->file_descriptor = fd;

    // Request thread safe access to the request queue
    pthread_mutex_lock(&queue_access);

    /* Check for anything in queue wait for an
       empty one signaled from req_queue_notfull */
    while (queue_position >= MAX_QUEUE_LEN) {
      pthread_cond_wait(&queue_full, &queue_access);
    }

    // Insert the request into the queue
    queue[queue_position] = cur_request;

    /* Update the queue index in a circular fashion,
       set the latest position for the request */
    queue_position = (queue_position + 1) % MAX_QUEUE_LEN;
    queue_size++;

    // Release the lock on the request queue, signal the queue is not empty
    pthread_cond_signal(&queue_not_empty);
    pthread_mutex_unlock(&queue_access);
    free(buffer);
  }
  return NULL;
}

void * worker(void *thread_id) {
  int* ID = (int*) thread_id;
  while (1) {
    /*Request thread safe access to the request queue
      by getting the req_queue_mutex lock*/
    pthread_mutex_lock(&queue_access);

    /*While the request queue is empty conditionally wait for the request queue
      lock once the not empty signal is raised*/
    while (queue_size == 0) {
      pthread_cond_wait(&queue_not_empty, &queue_access);
    }

    /*Now that you have the lock AND the queue is not empty,
      read from the request queue */
    request_t *requested_image = queue[req_index];

    // Update the request queue remove index in a circular fashion
    // Increase our request index in a circular manner
    req_index = (req_index + 1) % MAX_QUEUE_LEN;

    // Decrease the queue size since the worker finished the request
    queue_size--;

    /* Fire the request queue not full signal 
       to indicate the queue has a slot opened up 
       and release the request queue lock */
    pthread_cond_signal(&queue_full);
    pthread_mutex_unlock(&queue_access);

    // Match the image, send the closest image to the client
    database_entry_t image =
      image_match(requested_image->buffer, requested_image->file_size);
    send_file_to_client(
      requested_image->file_descriptor, image.buffer, image.file_size);

    // Print the results in server_log
    logfile = fopen("server_log", "a");
    LogPrettyPrint(
      logfile, *ID, req_index, image.file_name, image.file_size);
    fclose(logfile);
  }
  return NULL;
}

int main(int argc , char *argv[]) {
  // Ensures correct usage of server
  if (argc != 6) {
    printf("usage: %s port path num_dispatcher num_workers queue_length \n",
           argv[0]);
    return -1;
  }

  int port            = -1;
  char path[BUFF_SIZE] = "no path set\0";
  num_dispatcher      = -1;                               // global variable
  num_worker          = -1;                               // global variable
  queue_len           = -1;                               // global variable

  port = atoi(argv[1]);
  strcpy(path, argv[2]);
  num_dispatcher = atoi(argv[3]);
  num_worker = atoi(argv[4]);
  queue_len = atoi(argv[5]);

  init(port);

  loadDatabase(path);

  // Creates our num_dispatchers and stores in dispatcher_thread[]
  for (int i = 0; i < num_dispatcher; i++) {
    thread_ids[i] = i;
    int dThread = pthread_create(
      &dispatcher_thread[i], NULL, dispatch, (void*) &thread_ids[i]);
    if (dThread != 0) {
      perror("Cannot make dispatcher");
      return -1;
    }
  }

  // Creates our num_workers and stores in worker_thread[]
  for (int i = 0; i < num_worker; i++) {
    thread_ids[i] = i;
    int dThread = pthread_create(
      &worker_thread[i], NULL, worker, (void *) &thread_ids[i]);
    if (dThread != 0) {
      perror("Cannot make worker");
      return -1;
    }
  }

  /* Wait for each of the threads to complete their work
     Threads (if created) will not exit (see while loop),
     keeps main from exiting */
  int i;
  for (i = 0; i < num_dispatcher; i++) {
    fprintf(stderr, "JOINING DISPATCHER %d \n", i);
    if ((pthread_join(dispatcher_thread[i], NULL)) != 0) {
      printf("ERROR : Fail to join dispatcher thread %d.\n", i);
    }
  }

  for (i = 0; i < num_worker; i++) {
    fprintf(stderr, "JOINING WORKER %d \n", i);
    if ((pthread_join(worker_thread[i], NULL)) != 0) {
      printf("ERROR : Fail to join worker thread %d.\n", i);
    }
  }
  fprintf(stderr, "SERVER DONE \n");  // will never be reached in SOLUTION
}
