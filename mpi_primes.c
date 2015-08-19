/*
 * File:     mpi_primes.c
 * Purpose:  Implement parallel merge sort on a distributed list
 *           of prime numbers
 * Input:
 *    n:     last number to check
 * Output:
 *    my_primes: sorted list of primes on process 0
 *
 * Compile:  mpicc -g -Wall -o mpi_primes mpi_primes.c -lm
 * Run:
 *    mpiexec -n <p> ./mpi_primes <n>
 *       - p: the number of processes
 *       - n: last number to check
 *
 * Notes:
 * 1.  Uses tree-structured communication to gather the distributed
 *     lists onto process 0.
 *
 * Name: Joseph Tanigawa
 * Date: 11/11/2012
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <mpi.h>

/* Local functions */
int Get_n(int argc, char* argv[]);
void Usage(char prog[]);
int Is_prime(int i);
int Smallest_power_two(int p);
void Trace_send_recv(int recv_amounts[], int prime_counts[], int* max_recv, int* prime_recv_total, int my_rank, int p);
void Merge(int** primes_p, int prime_count, int received_primes[], int received_count, int** temp_p);

/* Functions involving communication */
void Merge_sort(int my_primes[], int prime_count, int recv_amounts[], int max_recv, int prime_recv_total, int my_rank, int p, MPI_Comm comm);

/*-------------------------------------------------------------------*/
int main(int argc, char* argv[]) {

   int my_rank, n, p, i, prime_count, prime_recv_total, max_recv;
   int* my_primes;
   int* prime_counts;
   int* recv_amounts;
   MPI_Comm comm;
   MPI_Init(&argc, &argv);
   comm = MPI_COMM_WORLD;
   MPI_Comm_size(comm, &p);
   MPI_Comm_rank(comm, &my_rank);

   if (my_rank == 0) {
      n = Get_n(argc, argv);
   }
   MPI_Bcast(&n, 1, MPI_INT, 0, comm);
   if (n < 2) {
      if (my_rank == 0) {
         Usage(argv[0]);
      }
      MPI_Finalize();
      exit(0);  
   }
      
   my_primes = malloc(((n/(2 * p)) + 2) * sizeof(int));
   prime_count = 0;
   if (my_rank == 0) {
      my_primes[0] = 2;
      prime_count++;
   }
   for (i = ((2 * my_rank) + 3); i <= n; i += (2 * p)) {    
      if (Is_prime(i)) {
         my_primes[prime_count] = i;
         prime_count++;
      }
   }
   
   prime_counts = malloc(p * sizeof(int));
   MPI_Allgather(&prime_count, 1, MPI_INT, prime_counts, 1, MPI_INT, comm);
   max_recv = 0;
   prime_recv_total = 0;
   recv_amounts = calloc(Smallest_power_two(p), sizeof(int));
   Trace_send_recv(recv_amounts, prime_counts, &max_recv, &prime_recv_total, my_rank, p);

   Merge_sort(my_primes, (prime_counts[my_rank] - prime_recv_total), recv_amounts, max_recv, prime_recv_total, my_rank, p, comm);
   
   free(prime_counts);
   MPI_Finalize();
   return 0;
}  /* main */

/*-------------------------------------------------------------------
 * Function:    Get_n
 * Purpose:     Get the input value n
 * Input args:  argc:  number of command line args
 *              argv:  array of command line args
 */
int Get_n(int argc, char* argv[]) {

   if (argc != 2) { /* If n isn't given */
      return 1;
   }      
   /* Convert string to int */
   int n = strtol(argv[1], NULL, 10);
   return n;
}  /* Get_n */

/*-------------------------------------------------------------------
 * Function:  Usage
 * Purpose:   Print a brief message explaining how the program is run.
 *            The quit.
 * In arg:    prog:  name of the executable
 */
void Usage(char prog[]) {

   fprintf(stderr, "usage: %s <n>\n", prog);
   fprintf(stderr, "   n = max integer to test for primality\n");
   fprintf(stderr, "   n must be >= 2\n");
}  /* Usage */

/*-------------------------------------------------------------------
 * Function:   Is_prime
 * Purpose:    Determine whether the argument is prime
 * Input arg:  i
 * Return val: true (nonzero) if arg is prime, false (zero) otherwise
 */
int Is_prime(int i) {

   int j;
   for (j = 2; j <= sqrt(i); j++)
      if (i % j == 0)
         return 0;
   return 1;
}  /* Is_prime */

/*---------------------------------------------------------------
 * Function:    Smallest_power_two
 * Purpose:     Compute the smallest power of 2 >= p.
 * Input arg:
 *    p:        The number of processes in the communicator
 *
 * Return val:  The smallest value 2^k such that 2^k >= p.
 */
int Smallest_power_two(int p) {
   int temp = 1;

   while (temp < p) 
      temp *= 2;
   return temp;
} /* Smallest_power_two */

/*---------------------------------------------------------------
 * Function:    Trace_send_recv
 * Purpose:     Calculate total number of primes the process will receive,
 *              the maximum number of primes the process will ever receive at once
 *              and the exact number of primes the process will receive during 
 *              each receive.
 * In args:     my_rank, p: the usual
 *
 * In/out args: recv_amounts: array of amount of primess process receives at each receive
 *              prime_counts: updates list of prime counts for each process to counts after last receive
 *              max_recv: maximum amount of primes the process will ever receive from another process
 *              prime_recv_total: total number of primes the process will receive
 */
void Trace_send_recv(int recv_amounts[], int prime_counts[], int* max_recv, int* prime_recv_total, int my_rank, int p) {

   int divisor = 2, proc_diff = 1, partner, i_send, i = 0, j = 0, done = 0;

   while (!done && divisor <= (p * 2)) {
      
      i_send = my_rank % divisor;
      
      if (i_send == 0) {
         partner = my_rank + proc_diff;
         if (partner < p) {
            recv_amounts[j] = prime_counts[partner];
            *prime_recv_total += prime_counts[partner];
            if (*max_recv < prime_counts[partner]) {
               *max_recv = prime_counts[partner];
            }
            j++;
            for (i = 0; i < p; i += proc_diff) { /* Updates number of primes each process has */
               if ((i % divisor) && (i - proc_diff) > -1) {
                  prime_counts[i - proc_diff] += prime_counts[i];
               }
            }
         }
         divisor *= 2;
         proc_diff *= 2;
      }else{
         done = 1;
      }
   }
} /* Trace_send_recv */

/*-------------------------------------------------------------------
 * Function:  Merge_sort
 * Purpose:   Parallel merge sort:  starts with a distributed 
 *            collection of sorted lists, ends with a global
 *            sorted list on process 0.  Uses tree-structured
 *            communication.
 * In args:   my_primes: list of primes found by the process
 *            my_prime_count: number of primes found by the process
 *            recv_amounts: list containing the number of primes received
 *                          by the process on each level of the send/recv tree
 *            max_recv: maximum number of primes that will ever be sent to
 *                      the process during a send/recv
 *            prime_recv_total: Total number of primes process 
 *                              will receive in the tree          
 *            my_rank, p, comm: the usual                 
 */
void Merge_sort(int my_primes[], int my_prime_count, int recv_amounts[], int max_recv, int prime_recv_total, int my_rank, int p, MPI_Comm comm) {

   int divisor = 2, proc_diff = 1, partner, i_send, i = 0, done = 0;
   int* temp_receive = malloc(max_recv * sizeof(int));
   int* temp_merge = malloc((my_prime_count + recv_amounts[i]) * sizeof(int));
   
   while (!done && divisor <= (p * 2)) {
   
      i_send = my_rank % divisor;
      
      if (i_send) {
         partner = my_rank - proc_diff;
         MPI_Send(my_primes, my_prime_count, MPI_INT, partner, 0, comm); 
         done = 1;
         
      }else {  /* I'm receiving */
         partner = my_rank + proc_diff;
         if (partner < p) {
            MPI_Recv(temp_receive, recv_amounts[i], MPI_INT, partner, 0, comm, MPI_STATUS_IGNORE);
            Merge(&my_primes, my_prime_count, temp_receive, recv_amounts[i], &temp_merge);
            my_prime_count += recv_amounts[i];
            i++;   
            temp_merge = malloc((my_prime_count + recv_amounts[i]) * sizeof(int));
         }
         divisor *= 2;
         proc_diff *= 2;
      }
   } /* Merge_sort complete */
   if (my_rank == 0) { /* print out sorted list of primes */
      for (i = 0; i < my_prime_count; i++) {
         printf("%d ", my_primes[i]);
      }
      printf("\n");
   }
   free(temp_merge);
   free(my_primes);
   free(recv_amounts);
   free(temp_receive);
}  /* Merge_sort */

/*-------------------------------------------------------------------
 * Function:  Merge
 * Purpose:   Merge two sorted lists, primes_p, received primes.  Return result in primes_p.
 *            temp_p hold the merged lists.  Both primes_p and received_primes have size elements.
 * In args:   prime_count: number of primes in primes_p
 *            received_primes: list of primes received by the process
 *            received_count: number of primes in received_primes
 *
 * In/out arg: primes_p:  first input array, output array
 * Scratch:    temp_p:  temporary storage for merged list of primes
 */
void Merge(int** primes_p, int prime_count, int received_primes[], int received_count, int** temp_p) {
   
   int* primes = *primes_p;
   int* temp = *temp_p;
   int primes_i, received_primes_i, temp_i;
   primes_i = received_primes_i = temp_i = 0;
   
   while (primes_i < prime_count && received_primes_i < received_count) {
     if (primes[primes_i] <= received_primes[received_primes_i]) {
         temp[temp_i] = primes[primes_i];
         temp_i++; primes_i++;
      }else {
         temp[temp_i] = received_primes[received_primes_i];
         temp_i++; received_primes_i++;
      }
   }
   
   if (primes_i >= prime_count) {
      for (; temp_i < (prime_count + received_count); temp_i++, received_primes_i++) {
         temp[temp_i] = received_primes[received_primes_i];
      }
   }else {
      for (; temp_i < (prime_count + received_count); temp_i++, primes_i++) {
         temp[temp_i] = primes[primes_i];
      }
   }
   free(*primes_p);
   *primes_p = *temp_p;
} /* Merge */
