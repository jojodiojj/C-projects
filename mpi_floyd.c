/* File:     mpi_floyd.c
 * Purpose:  Implement Floyd's algorithm for solving the all-pairs shortest
 *           path problem:  find the length of the shortest path between each
 *           pair of vertices in a directed graph.
 *
 * Input:    n, the number of vertices in the digraph
 *           temp_mat, the adjacency matrix of the digraph
 * Output:   A matrix showing the costs of the shortest paths
 *
 * Compile:  mpicc -g -Wall -o mpi_floyd mpi_floyd.c
 *           
 * Run:      mpiexec -n <number of processes> ./mpi_floyd < <matrix file>
 *           
 * Notes:
 * 1.  The input matrix is overwritten by the matrix of lengths of shortest
 *     paths.
 * 2.  Edge lengths should be nonnegative.
 * 3.  If there is no edge between two vertices, the length is the constant
 *     INFINITY.  So input edge length should be substantially less than
 *     this constant.
 * 4.  The cost of travelling from a vertex to itself is 0.  So the adjacency
 *     matrix has zeroes on the main diagonal.
 * 5.  No error checking is done on the input.
 * 6.  The adjacency matrix is stored as a 1-dimensional array and subscripts
 *     are computed using the formula:  the entry in the ith row and jth
 *     column is mat[i*n + j]
 *
 * Name:    Joseph Tanigawa
 * Date:    10/24/2012
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"

const int INFINITY = 1000000;

void Read_matrix(int mat[], int n);
void Print_matrix(int mat[], int n);
void Floyd(int local_mat[], int my_rank, int n, int p);
void Print_row(int local_mat[], int n, int my_rank, int i);

int main(void) {

   int p, my_rank, n;
   MPI_Comm comm;
   int* local_mat;
   int* temp_mat;
   MPI_Init(NULL, NULL);
   comm = MPI_COMM_WORLD;
   MPI_Comm_size(comm, &p);
   MPI_Comm_rank(comm, &my_rank);

   if (my_rank == 0) {
   
      printf("How many vertices?\n");
      scanf("%d", &n);
   }
   MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
   local_mat = malloc((n*n/p)*sizeof(int));
   
   if (my_rank == 0) {
   
      temp_mat = malloc((n*n)*sizeof(int));
      printf("Enter the matrix\n");
      Read_matrix(temp_mat, n);
   }
   MPI_Scatter(temp_mat, n*n/p, MPI_INT, local_mat, n*n/p, MPI_INT, 0, comm);
   Floyd(local_mat, my_rank, n, p);
   MPI_Gather(local_mat, n*n/p, MPI_INT, temp_mat, n*n/p, MPI_INT, 0, comm);
   free(local_mat);
   
   if (my_rank == 0) {
   
      printf("The solution is:\n");
      Print_matrix(temp_mat, n);
      free(temp_mat);
   }
   MPI_Finalize();
   return 0;
}  /* main */

/*-------------------------------------------------------------------
 * Function:  Read_matrix
 * Purpose:   Read in the adjacency matrix
 * In arg:    n
 * Out arg:   mat
 */
void Read_matrix(int mat[], int n) {
   int i, j;

   for (i = 0; i < n; i++)
      for (j = 0; j < n; j++)
         scanf("%d", &mat[i*n+j]);
}  /* Read_matrix */

/*-------------------------------------------------------------------
 * Function:  Print_matrix
 * Purpose:   Print the contents of the matrix
 * In args:   mat, n
 */
void Print_matrix(int mat[], int n) {
   int i, j;

   for (i = 0; i < n; i++) {
      for (j = 0; j < n; j++)
         if (mat[i*n+j] == INFINITY)
            printf("i ");
         else
            printf("%d ", mat[i*n+j]);
      printf("\n");
   }
}  /* Print_matrix */

/*---------------------------------------------------------------------
 * Function:  Print_row
 * Purpose:   Convert a row of local_mat to a string and then print
 *            the row.  This tends to reduce corruption of output
 *            when multiple processes are printing.
 * In args:   all            
 */
void Print_row(int local_mat[], int n, int my_rank, int i){
   char char_int[100];
   char char_row[1000];
   int j, offset = 0;

   for (j = 0; j < n; j++) {
      if (local_mat[i*n + j] == INFINITY)
         sprintf(char_int, "i ");
      else
         sprintf(char_int, "%d ", local_mat[i*n + j]);
      sprintf(char_row + offset, "%s", char_int);
      offset += strlen(char_int);
   }  
   printf("Proc %d > row %d = %s\n", my_rank, i, char_row);
}  /* Print_row */

/*-------------------------------------------------------------------
 * Function:    Floyd
 * Purpose:     Apply Floyd's algorithm to the matrix local_mat
 * In arg:      my_rank: rank of process
 *              n: number of vertices
 *              p: number of processes
 * In/out arg:  local_mat:  on input, the local adjacency matrix, on output
 *              lengths of the shortest paths between each pair of
 *              vertices.
 */
void Floyd(int local_mat[], int my_rank, int n, int p) {

   int int_city, local_int_city, local_city1, city2, root, j;
   int row_int_city[n];
   
   for (int_city = 0; int_city < n; int_city++) {
   
      root = int_city/(n/p);
      
      if (my_rank == root) {
      
         local_int_city = int_city % (n/p);
         
         for (j = 0; j < n; j++) {
         
            row_int_city[j] = local_mat[local_int_city*n + j];
         }
      }
      MPI_Bcast(row_int_city, n, MPI_INT, root, MPI_COMM_WORLD);

      for (local_city1 = 0; local_city1 < n/p; local_city1++) {
      
         for (city2 = 0; city2 < n; city2++) {
            
            if (local_mat[local_city1*n + city2] > (local_mat[local_city1*n + int_city] + row_int_city[city2])) {
           
               local_mat[local_city1*n + city2] = (local_mat[local_city1*n + int_city] + row_int_city[city2]);
            }
         }
      }
   }
}  /* Floyd */
