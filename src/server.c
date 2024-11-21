#include "../include/server.h"
#include <dirent.h>

// /********************* [ Helpful Global Variables ] **********************/
int num_dispatcher = 0; //Global integer to indicate the number of dispatcher threads
int num_worker = 0;  //Global integer to indicate the number of worker threads. Less than N Requests. Less than N Requests
FILE *logfile;  //Global file pointer to the log file
int queue_len = 0; //Global integer to indicate the length of the queue

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

// Queue that holds our thread requests (Holds a struct to our image that we will use to select that image)
request_t *queue[MAX_QUEUE_LEN];
// Request index
int req_index = 0;
int queue_position = 0;
// Size of our queue
int queue_size = 0;

// Counter for loading the database
int counter = 0;

//Image Match to find the closest image
database_entry_t image_match(char *input_image, int size) {
  const char *closest_file = NULL;
  int closest_distance = INT_MAX;
  int closest_index = 0;
  for(int i = 0; i < num_data_entries; i++)
	{
		const char *current_file = database[i].buffer;
		int result = memcmp(input_image, current_file, size);

        if(result == 0)
        {
            return database[i];
        }
        else if(result < closest_distance)
        {
            closest_distance = result;
            closest_file     = current_file;
            closest_index = i;
        }
    }

  if(closest_file != NULL)
  {
      return database[closest_index];
  }
  else
  {
      printf("No closest file found.\n");
  }
}

//LogPrettyPrint prints out to our server log and to stdout
void LogPrettyPrint(FILE* to_write, int threadId, int requestNumber, char * file_name, int file_size) {
  
  if (to_write != -1) {
    // print out to server_log
    fprintf(to_write, "[%d][%d][%s][%d]\n", threadId, requestNumber, file_name, file_size);
  } else {
    printf("ERROR WRITING TO LOGFILE\n");
  }
    // prints to stdout
    printf("[%d][%d][%s][%d]\n", threadId, requestNumber, file_name, file_size);
}


/* Given by TA */
void loadDatabase(char *path) {
  struct dirent *entry;
  DIR *dir = opendir(path);
  if (dir == NULL) {
    perror("Opendir ERROR");
    exit(0);
  }
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && strcmp(entry->d_name, ".DS_Store") != 0) {
      sprintf(database[counter].file_name, "%s/%s", path, entry->d_name);
      FILE *fp1;
      unsigned char *buffer1 = NULL;
      long fileLength1;

      // Open the image files
      fp1 = fopen(database[counter].file_name, "rb");
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
      database[counter].buffer = buffer1;
      database[counter].file_size = fileLength1;
      counter++;
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
    if (fd < 0) {
      perror("Failed to get connection");
      exit(-1);
    }

    //Confirm Connection
    printf("Dispatcher accepted connection\n");

    char *buffer = get_request_server(fd, &file_size);
    if (!buffer || file_size == 0) {
      perror("Failed to get request");
      close(fd);
      continue;
    }

    //(1) Copy the filename from get_request_server into allocated memory to put on request queue
    request_t *cur_request = malloc(sizeof(request_t));

    // Need to initialize buffer since it's a pointer to an array in the struct
    cur_request->buffer = malloc(file_size);

    // Copy the contents to the request
    memcpy(cur_request->buffer, buffer, file_size);
    cur_request->file_size = file_size;
    cur_request->file_descriptor = fd;

    //(2) Request thread safe access to the request queue
    pthread_mutex_lock(&queue_access);

    //(3) Check for anything in queue... wait for an empty one which is signaled from req_queue_notfull
    while (queue_position >= MAX_QUEUE_LEN) {
      pthread_cond_wait(&queue_full, &queue_access);
    }

    //(4) Insert the request into the queue
    queue[queue_position] = cur_request;

    //(5) Update the queue index in a circular fashion (i.e. update on circular fashion which means when the queue is full we start from the beginning again)
    // Set the latest position for the request
    queue_position = (queue_position + 1) % MAX_QUEUE_LEN;
    queue_size++;

  
    //(6) Release the lock on the request queue and signal that the queue is not empty anymore
    pthread_cond_signal(&queue_not_empty);
    pthread_mutex_unlock(&queue_access);
    free(buffer);
  }
  return NULL;
}

void * worker(void *thread_id) {

  // You may use them or not, depends on how you implement the function
  int num_request = 0;                                    //Integer for tracking each request for printing into the log file
  int fileSize    = 0;                                    //Integer to hold the size of the file being requested
  void *memory    = NULL;                                 //memory pointer where contents being requested are read and stored
  int fd          = INVALID;                              //Integer to hold the file descriptor of incoming request
  char *mybuf;                                  //String to hold the contents of the file being requested




  //Description:      Get the id as an input argument from arg, set it to ID
  int ID = (int) thread_id;
  
  while (1) {
    //(1) Request thread safe access to the request queue by getting the req_queue_mutex lock
    pthread_mutex_lock(&queue_access);

    //(2) While the request queue is empty conditionally wait for the request queue lock once the not empty signal is raised
    while(queue_size == 0) {
      pthread_cond_wait(&queue_not_empty, &queue_access);
    }

    //(3) Now that you have the lock AND the queue is not empty, read from the request queue
    request_t *requested_image = queue[req_index];

    //(4) Update the request queue remove index in a circular fashion
    // Increase our request index in a circular manner
    req_index = (req_index + 1) % MAX_QUEUE_LEN;

    // Decrease the queue size since the worker finished the request
    queue_size--;

    //get our image using image match and send the image to the client
    database_entry_t image = image_match(requested_image->buffer, requested_image->file_size);
    int client_send = send_file_to_client(requested_image->file_descriptor, image.buffer, image.file_size);
    if (client_send == -1) {
      perror("Failed to send file to client");
      exit(-1);
    }

  
    // Print the results in server_log and stdout
    logfile = fopen("server_log", "a");
    LogPrettyPrint(logfile, ID, req_index, image.file_name, image.file_size);
    fclose(logfile);

    //(5) Fire the request queue not full signal to indicate the queue has a slot opened up and release the request queue lock
    pthread_cond_signal(&queue_full);
    pthread_mutex_unlock(&queue_access);

  }
  return NULL;
}

int main(int argc , char *argv[]) {
  if (argc != 6) {
    printf("usage: %s port path num_dispatcher num_workers queue_length \n", argv[0]);
    return -1;
  }

  int port            = -1;
  char path[BUFF_SIZE] = "no path set\0";
  num_dispatcher      = -1;                               //global variable
  num_worker          = -1;                               //global variable
  queue_len           = -1;                               //global variable

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
    int dThread = pthread_create(&dispatcher_thread[i], NULL, dispatch, thread_ids[i]);
    if (dThread != 0) {
      perror("Cannot make dispatcher");
      return -1;
    }
  }

  // Creates our num_workers and stores in worker_thread[]
  for (int i = 0; i < num_worker; i++) {
    thread_ids[i] = i;
    int dThread = pthread_create(&worker_thread[i], NULL, worker, thread_ids[i]);
    if (dThread != 0) {
      perror("Cannot make worker");
      return -1;
    }
  }

  // Wait for each of the threads to complete their work
  // Threads (if created) will not exit (see while loop), but this keeps main from exiting
  int i;
  for (i = 0; i < num_dispatcher; i++) {
    fprintf(stderr, "JOINING DISPATCHER %d \n",i);
    if ((pthread_join(dispatcher_thread[i], NULL)) != 0) {
      printf("ERROR : Fail to join dispatcher thread %d.\n", i);  
    }
  }

  for (i = 0; i < num_worker; i++) {
    fprintf(stderr, "JOINING WORKER %d \n",i);
    if ((pthread_join(worker_thread[i], NULL)) != 0) {
      printf("ERROR : Fail to join worker thread %d.\n", i);
    }
  }
  fprintf(stderr, "SERVER DONE \n");  // will never be reached in SOLUTION
}
