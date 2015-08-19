/* File:     lcs.cu
 * Purpose:  Use CUDA to find the longest common subsequence of two 
 *           input sequences of positive ints.  This version proceeds 
 *           down "anti-diagonals".  It stores all of subsequences:  not 
 *           just the subsequences from the previous two diagonals.
 *
 * Compile:  nvcc -g -G -arch=sm_21 -o lcs lcs.cu 
 * Run:      ./lcs <blocks> <threads_per_block>
 *
 * Input:    number of elements in the first sequence (m)
 *           elements of the first sequence
 *           number of elements in the second sequence (n)
 *           elements of the second sequence
 *
 * Output:   The length of the longest common subsequence and
 *           the actual sequence.
 *
 * Name:     Joseph Tanigawa
 *
 * Date:     4/14/2014
 */

#include <stdio.h>
#include <stdlib.h>
#include "timer.h"

void Usage(char progname[]);
void Handle_args(int argc, char* argv[], int* blocks, int* threads_per_block,
                 int* suppress);
void Get_sequences(int* m, int* n, int** seq1_h, int** seq2_h);        
void Print_results(int suppress, int* big_L, int m, int n, int lcs_max,
                   double elapsed);         
int  Get_seq_len(char prompt[]);
void Read_seq(char prompt[], int seq[], int m);
void Print_seq(char title[], int seq[], int m);

/*-------------------------------------------------------------------*/
/* Kernel to find L entry */
__global__ void Find_L_entry(int L_d[], int seq1_d[], int m, int seq2_d[],
                             int n, int max_diag_len, int diag) {
                             
    int j = threadIdx.x + blockIdx.x*blockDim.x;
    int i = diag - j;    
    int lim1j, lijm1;
    int max_seq = max_diag_len + 1;
    int* L = &L_d[(n+1)*max_seq + max_seq];
    
    if (i >= 0 && i < n)
        if (seq1_d[i] == seq2_d[j]) {
            /* Get previous seq length and increment by 1 */
            L[i*(n+1)*max_seq+j*max_seq] =
            L[(i-1)*(n+1)*max_seq+(j-1)*max_seq] + 1; 
            /* Copy prev seq into cur seq */
            int elem;
            for (elem = 0; elem < L[(i-1)*(n+1)*max_seq+(j-1)*max_seq];
                 elem++) {
                L[i*(n+1)*max_seq+j*max_seq + 1+elem] =
                L[(i-1)*(n+1)*max_seq+(j-1)*max_seq + 1+elem];  
            }      
            /* Add seq[i] to the new seq */
            L[i*(n+1)*max_seq+j*max_seq + 1+elem] = seq1_d[i];            
        } else {
            /* Get length of longest existing seq */
            if (i == 0) lim1j = 0;
            else lim1j = L[(i-1)*(n+1)*max_seq+j*max_seq];
            if (j == 0) lijm1 = 0;
            else lijm1 = L[i*(n+1)*max_seq+(j-1)*max_seq];
            
            if (lim1j >= lijm1) {
                L[i*(n+1)*max_seq+j*max_seq] = lim1j;
                if (lim1j != 0) /* Copy prev seq into cur seq  */
                    for (int elem = 0; elem < lim1j; elem++)
                         L[i*(n+1)*max_seq+j*max_seq + 1+elem] =
                         L[(i-1)*(n+1)*max_seq+j*max_seq + 1+elem];    
            } else {
                L[i*(n+1)*max_seq+j*max_seq] = lijm1;
                if (lijm1 != 0) /* Copy prev seq into cur seq  */
                    for (int elem = 0; elem < lijm1; elem++)
                         L[i*(n+1)*max_seq+j*max_seq + 1+elem] =
                         L[i*(n+1)*max_seq+(j-1)*max_seq + 1+elem];  
            }
        }      
}  /* Find_L_entry */


/* Host code */
int main(int argc, char* argv[]) {
    int blocks, threads_per_block, suppress, m, n, lcs_max;
    int diag_count, max_diag_len, diag, big_L_size;
    double start, finish;  
    
    /* host pointers */
    int* seq1_h;
    int* seq2_h;
    int* big_L_h;
    
    /* device pointers */
    int* seq1_d;
    int* seq2_d;
    int* big_L_d;

    Handle_args(argc, argv, &blocks, &threads_per_block, &suppress);
    Get_sequences(&m, &n, &seq1_h, &seq2_h);   
    
    if (m < n)
       max_diag_len = lcs_max = m;
    else
       max_diag_len = lcs_max = n;
       
    diag_count = m + n - 1;
    big_L_size = (m+1)*(n+1)*(lcs_max+1);
    big_L_h = (int*) calloc(big_L_size, sizeof(int));
    
    /* Allocate seq1, seq2 & big_L in device memory */
    cudaMalloc(&seq1_d, m*sizeof(int));
    cudaMalloc(&seq2_d, n*sizeof(int));
    cudaMalloc(&big_L_d, big_L_size*sizeof(int));  
        
    /* Copy vectors from host memory to device memory */
    cudaMemcpy(seq1_d, seq1_h, m*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(seq2_d, seq2_h, n*sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(big_L_d, big_L_h, big_L_size*sizeof(int),
               cudaMemcpyHostToDevice);
    
    GET_TIME(start);
    for (diag = 0; diag < diag_count; diag++) {
        Find_L_entry<<<blocks, threads_per_block>>>(big_L_d, seq1_d, m, seq2_d,
                                                    n, max_diag_len, diag);
        cudaThreadSynchronize();
    }
    GET_TIME(finish);
    
    cudaMemcpy(big_L_h, big_L_d, big_L_size*sizeof(int),
               cudaMemcpyDeviceToHost);
    
    Print_results(suppress, big_L_h, m, n, lcs_max, finish-start);
    
    /* Free device memory */
    cudaFree(seq1_d);
    cudaFree(seq2_d);
    cudaFree(big_L_d);
    
    /* Free host memory */
    free(seq1_h);
    free(seq2_h);
    free(big_L_h);
    return 0;
}   /* main */


/*-----------------------------------------------------------------------------
 * Function:  Usage
 * Purpose:   Print the command line for starting the program and exit
 * In arg:    Name of executable
 */
void Usage(char progname[]) {
   fprintf(stderr, "usage: %s <blocks> <threads_per_block>", progname);
   fprintf(stderr, " <'n'(optional)>\n");
   fprintf(stderr, "The last argument supresses output of the");
   fprintf(stderr, " L data structure\n");
   exit(0);
}  /* Usage */


/*-----------------------------------------------------------------------------
 * Function:     Handle_args
 * Purpose:      Read command line arguments
 * In args:      argc, argv
 * In/out args:  blocks, threads_per_block, suppress
 */
void Handle_args(int argc, char* argv[], int* blocks, int* threads_per_block,
                 int* suppress) {
                  
    if (argc < 3) Usage(argv[0]);
    *blocks = strtol(argv[1], NULL, 10);
    *threads_per_block = strtol(argv[2], NULL, 10);
    
    if (argc == 4 && *argv[3] == 'n')
        *suppress = 1;
    else if (argc == 4 && *argv[3] != 'n')
        Usage(argv[0]);    
    else
        *suppress = 0;
}


/*-----------------------------------------------------------------------------
 * Function:     Get_sequences
 * Purpose:      Read in size of sequences and the sequences through user input
 * In args:      argc, argv
 * In/out args:  blocks, threads_per_block, suppress
 */
 void Get_sequences(int* m, int* n, int** seq1_h, int** seq2_h) {
 
    *m = Get_seq_len("first sequence");
    *seq1_h = (int*) malloc(*m*sizeof(int));
    Read_seq("first sequence", *seq1_h, *m);
    Print_seq("1", *seq1_h, *m);
    *n = Get_seq_len("second sequence");
    *seq2_h = (int*) malloc(*n*sizeof(int));
    Read_seq("second sequence", *seq2_h, *n);
    Print_seq("2", *seq2_h, *n);
    printf("\n");
} 


/*-----------------------------------------------------------------------------
 * Function:     Print_results
 * Purpose:      Print results to screen including L if necessary
 * In args:      suppress, big_L, m, n, lcs_max, elapsed
 */
void Print_results(int suppress, int* big_L, int m, int n, int lcs_max,
                   double elapsed) {
                   
    int i, j, L_size, p;
    int* L;
    int* lcs;
    
    L_size = m*n*(lcs_max+1);
    L = &big_L[1*(n+1)*(lcs_max+1) + 1*(lcs_max+1)];
    if (suppress == 0) {
        printf("L =\n");
        for (i = 0; i < L_size+(m-1)*(lcs_max+1); i+=(lcs_max+1)) {
            for (j = i; j < i+lcs_max+1; j++)
                printf("%d ", L[j]);
            printf("\n");
        }    
        printf("\n");
    }
    
    p = L[(m-1)*(n+1)*(lcs_max+1) + (n-1)*(lcs_max+1)];
    lcs = &L[(m-1)*(n+1)*(lcs_max+1) + (n-1)*(lcs_max+1) + 1];       
    printf("The longest common subsequence contains %d elements\n", p);
    Print_seq("longest common", lcs, p);
    printf("Elapsed time for determining LCS = %e seconds\n", elapsed);                   
}


/*-----------------------------------------------------------------------------
 * Function:  Get_seq_len
 * Purpose:   Prompt for the length of a sequence, read it from stdin 
 *            and return it
 * In arg:    prompt:  the name of the sequence
 * Ret val:   the length of the sequence
 */
int Get_seq_len(char prompt[])  {
    int len;

    printf("How many elements in %s?\n", prompt);
    scanf("%d", &len);
    return len;
}   /* Get_seq_len */


/*-------------------------------------------------------------------
 * Function:  Read_seq
 * Purpose:   Read the elements of a sequence from stdin
 * In args:   prompt:  name of the sequence
 *            m:  number of elements in the sequence
 * Out arg:   seq:  the elements
 */
void Read_seq(char prompt[], int seq[], int m) {
   int i;
   printf("Enter the elements of %s\n", prompt);
   for (i = 0; i < m; i++)
      scanf("%d", &seq[i]);
}  /* Read_seq */


/*-------------------------------------------------------------------
 * Function:  Print_seq
 * Purpose:   Print the elements of a sequence to stdout
 * In args:   title:  name of the sequence
 *            seq:  the sequence
 *            m:  the number of elements in the sequence
 */
void Print_seq(char title[], int seq[], int m) {
   int i;
   printf("Sequence %s:\n", title);
   for (i = 0; i < m; i++)
      printf("%d ", seq[i]);
   printf("\n");
}  /* Print_seq */
