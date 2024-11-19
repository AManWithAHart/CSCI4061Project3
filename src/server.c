#include "../include/server.h"
#include <dirent.h>

// /********************* [ Helpful Global Variables ] **********************/
int num_dispatcher = 0; //Global integer to indicate the number of dispatcher threads
int num_worker = 0;  //Global integer to indicate the number of worker threads. Less than N Requests. Less than N Requests
FILE *logfile;  //Global file pointer to the log file
int queue_len = 0; //Global integer to indicate the length of the queue

#define WORKER_DISPATCHER 100

/* TODO: Intermediate Submission
  TODO: Add any global variables that you may need to track the requests and threads
  [multiple funct]  --> How will you track the p_thread's that you create for workers?
  [multiple funct]  --> How will you track the p_thread's that you create for dispatchers?
  [multiple funct]  --> Might be helpful to track the ID's of your threads in a global array
  What kind of locks will you need to make everything thread safe? [Hint you need multiple]
  What kind of CVs will you need  (i.e. queue full, queue empty) [Hint you need multiple]
  How will you track the number of images in the database?
  How will you track the requests globally between threads? How will you ensure this is thread safe? Example: request_t req_entries[MAX_QUEUE_LEN];
  [multiple funct]  --> How will you update and utilize the current number of requests in the request queue?
  [worker()]        --> How will you track which index in the request queue to remove next?
  [dispatcher()]    --> How will you know where to insert the next request received into the request queue?
  [multiple funct]  --> How will you track the p_thread's that you create for workers? TODO
  How will you store the database of images? What data structure will you use? Example: database_entry_t database[100];
*/
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



//TODO: Implement this function
/**********************************************
 * image_match
   - parameters:
      - input_image is the image data to compare
      - size is the size of the image data
   - returns:
       - database_entry_t that is the closest match to the input_image
************************************************/
//just uncomment out when you are ready to implement this function
database_entry_t image_match(char *input_image, int size) {
  int closest_distance = INT_MAX;
  int closest_index = -1;

  for (int i = 0; i < num_data_entries; i++) {
		int distance = 0;
    const char *db_image = database[i].buffer;

    int comp_size;
    
    if (size < database[i].file_size) {
      comp_size = size;
    } else {
      comp_size = database[i].file_size;
    }

    for (int j = 0; j < comp_size; j++) {
      distance += abs((unsigned char) input_image[j] - (unsigned char)db_image[i]);
    }

    distance += abs(size - database[i].file_size);

    if (distance < closest_distance) {
      closest_distance = distance;
      closest_index = i;
    }
  }

  if (closest_index >= 0) {
    printf("This is the closest index: %d\n", closest_index);
    return database[closest_index];
  } 

  printf("No closest file found.\n");
  return database[0];
}

//TODO: Implement this function
/**********************************************
 * LogPrettyPrint
   - parameters:
      - to_write is expected to be an open file pointer, or it
        can be NULL which means that the output is printed to the terminal
      - All other inputs are self explanatory or specified in the writeup
   - returns:
       - no return value
************************************************/
void LogPrettyPrint(FILE* to_write, int threadId, int requestNumber, char * file_name, int file_size) {
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
    fprintf(to_write,"****** Server Log Begin ******\n");
    fprintf(to_write, "Thread ID: %d\n", threadId);
    fprintf(to_write,"Request Number: %d\n", requestNumber);
    fprintf(to_write,"File Name: %s\n", file_name);
    fprintf(to_write,"File Size: %d\n", file_size);
    fprintf(to_write,"****** Server Log End ******\n");
    fprintf(to_write, "\n");
  }
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
    memcmp(cur_request->buffer, buffer, file_size);
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


  /* TODO : Intermediate Submission
  *    Description:      Get the id as an input argument from arg, set it to ID
  */

  // pthread_t *ID = (pthread_t*) thread_id;
  int* ID = (int*) thread_id;
  
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

    //(5) Fire the request queue not full signal to indicate the queue has a slot opened up and release the request queue lock
    pthread_cond_signal(&queue_full);
    pthread_mutex_unlock(&queue_access);

    /* TODO
    *    Description:       Call image_match with the request buffer and file size
    *    store the result into a typeof database_entry_t
    *    send the file to the client using send_file_to_client(int fd, char * buffer, int size)
    */
    printf("Matching image\n");
    printf("Buffer: %s; Size: %d\n", requested_image->buffer, requested_image->file_size);
    database_entry_t image = image_match(requested_image->buffer, requested_image->file_size);
    send_file_to_client(requested_image->file_descriptor, image.buffer, image.file_size);

    /* TODO
    *    Description:       Call LogPrettyPrint() to print server log
    *    update the # of request (include the current one) this thread has already done, you may want to have a global array to store the number for each thread
    *    parameters passed in: refer to write up
    */
    LogPrettyPrint(logfile, &thread_id, req_index, "TBD", image.file_size);
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

  logfile = fopen("server_log", "a");

  init(port);

  loadDatabase(path);

  /* TODO: Intermediate Submission
  *    Description:      Create dispatcher and worker threads
  *    Hints:            Use pthread_create, you will want to store pthread's globally
  *                      You will want to initialize some kind of global array to pass in thread ID's
  *                      How should you track this p_thread so you can terminate it later? [global]
  */
  // Creates our num_dispatchers and stores in dispatcher_thread[]
  for (int i = 0; i < num_dispatcher; i++) {
    thread_ids[i] = i;
    int dThread = pthread_create(&dispatcher_thread[i], NULL, dispatch, (void*) &thread_ids[i]);
    if (dThread != 0) {
      perror("Cannot make dispatcher");
      return -1;
    }
  }

  // Creates our num_workers and stores in worker_thread[]
  for (int i = 0; i < num_worker; i++) {
    thread_ids[i] = i;
    int dThread = pthread_create(&worker_thread[i], NULL, worker, (void *) &thread_ids[i]);
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
  fclose(logfile);//closing the log files
}
