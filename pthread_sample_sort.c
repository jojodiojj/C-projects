/* File:     pthread_sample_sort.c
 * Purpose:  Completes Sample Sort algorithm on a list of integers
 * Input:    thread_count: number of threads
 *           sample size: size of sample each thread takes from input list
 *           list size: size of input list
 *           input file: name of input file
 * Output:   The sorted array and elapsed time for sort
 * Compile:  gcc -g -Wall -o pthread_sample_sort pthread_sample_sort.c -lpthread
 * Run:      ./pthread_sample_sort <thread_count> <sample size> <list size> 
 *                   <input file> <optional'n' to suppress sorted array>
 *
 * Name: Joseph Tanigawa
 * Date: 3/23/2014
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <math.h>
#include "timer.h"
/*---------------------------------------------------------------------------*/
/* Global variables */
FILE*  key_file;
int thread_count, sample_size, n, suppress, local_n, local_s;
double* all_elapsed;
int* splitters;
int* buckets;
int* samples;
int* sorted_samples;
int* C;
int* PSRC;
int* CSC;
int* PSCSC;
int* D;
pthread_barrier_t barrier;
/*---------------------------------------------------------------------------*/
void  Handle_args(int argc, char* argv[]);
void  Usage(char* prog_name);
void  Allocate_arrays(void);
void  Read_keys(FILE* key_file);
void  Free_arrays(void);
void  Print_results(void);
void* Sample_sort(void* rank);
void  Get_samples(long my_rank, int my_bucket[], int my_min_subscript, 
                 int used_subscripts[]);
int   Is_used(int subscript, int used_subscripts[], int subscript_count);
void  Sort_samples(int my_min_subscript);
void  Fill_C(int my_bucket[], int my_C[]);
void  Get_PSRC_CSC_PSCSC(long my_rank, int my_PSRC[], int my_C[]);
void  Fill_my_D(long my_rank, int my_D[]);
int   compare(const void * a, const void * b);
/*---------------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
    pthread_t* thread_handles;
    long thread;

    Handle_args(argc, argv);
    Allocate_arrays();
    Read_keys(key_file);
    fclose(key_file);
    
    local_n = n/thread_count;
    local_s = sample_size/thread_count;
    
    thread_handles = malloc(thread_count * sizeof(pthread_t));
    pthread_barrier_init(&barrier, NULL, thread_count);

    for (thread = 0; thread < thread_count; thread++)
        pthread_create(&thread_handles[thread], NULL, Sample_sort,
                       (void*) thread);

    for (thread = 0; thread < thread_count; thread++)
        pthread_join(thread_handles[thread], NULL);
    pthread_barrier_destroy(&barrier);
    free(thread_handles);
    
    Print_results();
    Free_arrays();
    return 0;
}  /* main */


/*-----------------------------------------------------------------------------
 * Function:            Handle_args
 * Purpose:             Handle command line arguments
 * In arg:              argc, argv
 * Global vars in/out:  thread_count, sample_size, n, suppress, key_file
 */
void Handle_args(int argc, char* argv[]) {
    if (argc < 5) Usage(argv[0]);
    thread_count = strtol(argv[1], NULL, 10);
    sample_size = strtol(argv[2], NULL, 10);
    n = strtol(argv[3], NULL, 10);
    if (thread_count < 1 || n <= thread_count || sample_size > n)
        Usage(argv[0]);
    if (argc == 6) 
        if (*argv[5] == 'n')
            suppress = 1;
        else
            Usage(argv[0]);
    else
        suppress = 0;
    key_file = fopen(argv[4], "r");
    if (key_file == NULL) {
       fprintf(stderr, "Can't open %s\n", argv[4]);
       Usage(argv[0]);
    }
}   /* Handle_args */


/*-----------------------------------------------------------------------------
 * Function:  Usage
 * Purpose:   Inform user how to start program and exit
 * In arg:    prog_name
 */
void Usage(char* prog_name) {
    fprintf(stderr, "usage: %s <thread_count> <sample size> ", prog_name);
    fprintf(stderr, "<list size> <input file> <'n'(optional)>\n");
    fprintf(stderr, "thread_count must be greater than or equal to 1\n");
    fprintf(stderr, "last argument supresses output of sorted list\n");
    exit(0);
}  /* Usage */


/*-----------------------------------------------------------------------------
 * Function:         Allocate_arrays
 * Purpose:          Allocate memory for all arrays
 * Global vars in:   all_elapsed, buckets, samples, sorted_samples, splitters
 *                   C, PSRC, CSC, PSCSC, D, n, thread_count, sample_size
 * Global vars out:  all_elapsed, buckets, samples, sorted_samples, splitters
 *                   C, PSRC, CSC, PSCSC, D                            
 */
void Allocate_arrays(void) {
    all_elapsed = malloc(thread_count * sizeof(double));
    buckets = malloc(n * sizeof(int));
    samples = malloc(sample_size * sizeof(int));
    sorted_samples = malloc(sample_size * sizeof(int));
    splitters = malloc((thread_count - 1) * sizeof(int));
    C = (int*) calloc((thread_count * thread_count), sizeof(int));
    PSRC = (int*) calloc((thread_count * thread_count), sizeof(int));
    CSC = (int*) calloc(thread_count, sizeof(int));
    PSCSC = (int*) calloc(thread_count, sizeof(int));
    D = (int*) calloc(n, sizeof(int));
}   /* Allocate_arrays */


/*-----------------------------------------------------------------------------
 * Function:         Read_keys
 * Purpose:          Read in the keys
 * In arg:           key_file
 * Global vars in:   buckets, n
 * Global vars out:  buckets
 */
void Read_keys(FILE* key_file) {
    int i;
    for (i = 0; i < n; i++)
        fscanf(key_file, "%d", &buckets[i]);
}  /* Read_keys */


/*-----------------------------------------------------------------------------
 * Function:           Free_arrays
 * Purpose:            Frees memory for all arrays
 * Global vars in:     all_elapsed, buckets, samples, sorted_samples, splitters
 *                     C, PSRC, CSC, PSCSC, D                           
 */
void Free_arrays(void) {
    free(all_elapsed);
    free(buckets);
    free(samples);
    free(sorted_samples);
    free(splitters);
    free(C);
    free(PSRC);
    free(CSC);
    free(PSCSC);
    free(D);
}   /* Free_arrays */


/*-----------------------------------------------------------------------------
 * Function:         Print_results
 * Purpose:          Print out final sorted array and or total elapsed time
 * Global vars in:   suppress, n, thread_count, D, all_elapsed
 */
void Print_results(void) {
    double max_time;
    int i;
    if (suppress == 0) {
        printf("\nResult of Sample Sort:\n");
        for (i = 0; i < n; i++)
            printf("%d ", D[i]);
        printf("\n");
    }
    max_time = all_elapsed[0];
    for (i = 1; i < thread_count; i++) 
        if (all_elapsed[i] > max_time)
            max_time = all_elapsed[i];
    printf("Elapsed time for Sample Sort = %e seconds\n", max_time);
}   /* Print_results */


/*-----------------------------------------------------------------------------
 * Function:            Sample_sort
 * Purpose:             Parallel sample sort
 * Global vars in:      all_elapsed, buckets, samples, sorted_samples, C, PSRC, 
 *                      splitters, CSC, PSCSC, D, barrier, local_n, local_s
 * Global vars out:     all_elapsed, splitters
 * Variables in:        rank
 */
void* Sample_sort(void* rank) {   
    long my_rank = (long) rank;
    int my_min_subscript;
    double start, finish;
    int* used_subscripts;
    int* my_bucket;
    int* my_C;
    int* my_PSRC;
    int* my_D;
    
    my_min_subscript = my_rank * local_s;
    used_subscripts = malloc(sample_size*sizeof(int));
    my_bucket = buckets + my_rank*local_n;
    my_C = C + my_rank*thread_count;
    my_PSRC = PSRC + my_rank*thread_count;
    start = finish = 0.0;
    my_D = NULL;
    
    pthread_barrier_wait(&barrier);
    GET_TIME(start);
    
    Get_samples(my_rank, my_bucket, my_min_subscript, used_subscripts);    
    Sort_samples(my_min_subscript);
    
    if (my_rank > 0)
        splitters[my_rank - 1] = (sorted_samples[my_min_subscript] + 
                                  sorted_samples[my_min_subscript - 1])/2; 
    qsort(my_bucket, local_n, sizeof(int), compare);
    pthread_barrier_wait(&barrier);

    Fill_C(my_bucket, my_C);
    Get_PSRC_CSC_PSCSC(my_rank, my_PSRC, my_C);
      
    if (my_rank == 0)
        my_D = D;
    else
        my_D = D + PSCSC[my_rank-1]; 
    Fill_my_D(my_rank, my_D);
    qsort(my_D, CSC[my_rank], sizeof(int), compare);
    GET_TIME(finish);
    all_elapsed[my_rank] = finish - start;
    free(used_subscripts);
    return NULL;
}   /* Samples */


/*-----------------------------------------------------------------------------
 * Function:            Get_samples
 * Purpose:             Get random samples from my_bucket
 * Global vars in:      samples, barrier, local_s
 * Variables in:        rank, my_bucket, my_min_subscript, used_subscripts
 * Global vars out:     samples
 */
void Get_samples(long my_rank, int my_bucket[], int my_min_subscript, 
                 int used_subscripts[]) {
    int subscript_count, subscript, i;
    subscript_count = 0;
    srandom(my_rank+1);
    for (i = my_min_subscript; i < my_min_subscript + local_s; i++) {
        do {
            subscript = (random() % local_n);
        } while (Is_used(subscript, used_subscripts, subscript_count));       
        used_subscripts[subscript_count] = subscript;
        subscript_count++;
        samples[i] = my_bucket[subscript];
    }
    pthread_barrier_wait(&barrier);
}   /* Get_samples */


/*-----------------------------------------------------------------------------
 * Function:        Is_used
 * Purpose:         Check if subscript has already been used
 * Variables in:    subscript, used_subscripts, subscripts_count
 * vars out:        returns 1 if subscript has been used else zero
 */
int Is_used(int subscript, int used_subscripts[], int subscript_count) {
    int i;
    for (i = 0; i < subscript_count; i++)
        if (subscript == used_subscripts[i]) return 1;
    return 0;
}   /* Is_used */


/*-----------------------------------------------------------------------------
 * Function:        Sort_samples
 * Purpose:         Sort list of samples
 * Global vars in:  local_s, sample_size, samples, sorted_samples, barrier
 * Global vars out: sorted_samples
 */
void Sort_samples(int my_min_subscript) {
    int i, j, less_than;
    for (i = my_min_subscript; i < my_min_subscript + local_s; i++) {
        less_than = 0;
        for (j = 0; j < sample_size; j++) 
            if (samples[j] < samples[i])
                less_than++;
            else if (samples[j] == samples[i] && i < j)
                less_than++;        
        sorted_samples[less_than] = samples[i];   
    }
    pthread_barrier_wait(&barrier);
}   /* Sort_samples */


/*-----------------------------------------------------------------------------
 * Function:        Fill_C
 * Purpose:         Fill C with counts of how many ints in my_bucket the other
 *                  threads will recieve 
 * Global vars in:  splitters, local_n, barrier, thread_count
 * Vars in/out:     my_bucket, my_C
 */
void Fill_C(int my_bucket[], int my_C[]) {
    int i, j, found_thread;
    found_thread = 0;
    for (i = 0; i < local_n; i++) {
        if (my_bucket[i] < splitters[0]) 
            my_C[0]++;
        else {
            for (j = 1; j < thread_count - 1; j++) {
                if (my_bucket[i] < splitters[j]) {
                    my_C[j]++;
                    found_thread = 1;
                    break;
                }
            }
            if (found_thread == 0) 
                my_C[thread_count - 1]++;
            else
                found_thread = 0;
        }   
    }
    pthread_barrier_wait(&barrier);    
}   /* Fill_C */


/*-----------------------------------------------------------------------------
 * Function:        Get_PSRC_CSC_PSCSC
 * Purpose:         Determines PSRC, CSC, PSCSC
 * Global vars in:  C, thread_count, PSCSC, CSC, barrier
 * Global vars out: CSC, PSCSC
 * Vars in:         my_rank, my_PSRC, my_C
 * Vars out:        my_PSRC
 */
void Get_PSRC_CSC_PSCSC(long my_rank, int my_PSRC[], int my_C[]) {
    int i;
    my_PSRC[0] = my_C[0];
    CSC[my_rank] = C[my_rank];
    for (i = 1; i < thread_count; i++) {
        my_PSRC[i] = my_PSRC[i-1] + my_C[i];
        PSCSC[my_rank] = CSC[my_rank] += C[i*thread_count+my_rank];
    }    
    pthread_barrier_wait(&barrier);
    
    if (my_rank == 0) 
        for (i = 1; i < thread_count; i++)
            PSCSC[i] += PSCSC[i-1];
    pthread_barrier_wait(&barrier);
}   /* Get_PSRC_CSC_PSCSC */


/*-----------------------------------------------------------------------------
 * Function:        Fill_my_D
 * Purpose:         Determines PSRC, CSC, PSCSC
 * Global vars in:  local_n, thread_count, PSRC, C, buckets
 * Vars in:         my_rank, my_D
 * Vars out:        my_D
 */
void Fill_my_D(long my_rank, int my_D[]) {
    int i, j, offset, elt;
    i = 0;
    for (j = 0; j < thread_count; j++) {
        if (my_rank == 0) 
            offset = j * local_n;
        else    
            offset = j*local_n + PSRC[j*thread_count+(my_rank-1)];    
        for (elt = 0; elt < C[j*thread_count+my_rank]; elt++)
             my_D[i++] = buckets[offset+elt];
    }
}   /* Fill_my_D */


int compare(const void * a, const void * b) {
    if (*(int*)a > *(int*)b) return 1;
    else if (*(int*)a < *(int*)b) return -1;
    else return 0;
}   /* compare */
