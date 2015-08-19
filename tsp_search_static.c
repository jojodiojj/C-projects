/* File:     tsp_search_static.c
 * Purpose:  A multi threaded breath-first search to solve an 
 *           instance of the travelling salesman problem.
 * Input:    From a user-specified file, the number of cities
 *           followed by the costs of travelling between the
 *           cities organized as a matrix:  the cost of
 *           travelling from city i to city j is the ij entry.
 *	     thread_count: number of threads to split the two city pairs between
 * Output:   The best tour found by the program and the cost
 *           of the tour.
 * Compile:  gcc -g -Wall -o tsp_search_static tsp_search_static.c -lpthread
 * Run:      ./tsp_search_static <thread_count> <matrix file>
 *
 * Notes:
 * 1.  Weights and cities are non-negative ints.
 * 2.  Program assumes the cost of travelling from a city to
 *     itself is zero, and the cost of travelling from one
 *     city to another city is positive.
 * 3.  Note that costs may not be symmetric:  the cost of travelling
 *     from A to B, will, in general, be different from the cost
 *     of travelling from B to A.
 * 4.  Salesperson's home town is 0.
 * 5.  This version uses an array for the stack.
 *
 * Name: Joseph Tanigawa
 * Date: 12/3/2012
 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

const int INFINITY = 1000000;
const int NO_CITY = -1;
const int FALSE = 0;
const int TRUE = 1;

typedef int city_t;
typedef int weight_t;

typedef struct {
   city_t* cities;
   int count;
   weight_t cost;
} tour_t;

typedef struct stack_struct {
   tour_t* tour_p;               /* Partial tour             */
   city_t  city;                 /* City under consideration */
   weight_t cost;                /* Cost of going to city    */
} stack_elt_t;

/*------------------------------------------------------------------*/
/* Global variables */
int thread_count;
int n;
weight_t* mat;
tour_t best_tour;
pthread_mutex_t mutex;

/*------------------------------------------------------------------*/
void Usage(char* prog_name);
void Read_mat(FILE* mat_file);

void Initialize_tour(tour_t* tour_p);
void* Search(void* rank);
void Fill_stack(stack_elt_t*** stack_p, int* top_p, long my_rank, tour_t* temp_tour_p);
int  Empty(int* top_p);
void Pop(tour_t** tour_pp, city_t* city_p, weight_t* cost_p, stack_elt_t*** stack_p, int* top_p);
void Check_best_tour(city_t city, tour_t* tour_p, tour_t* local_best_p);
int  Feasible(city_t city, city_t nbr, tour_t* tour_p, tour_t* local_best_p);
int  Visited(city_t nbr, tour_t* tour_p);
void Push(tour_t* tour_p, city_t city, weight_t cost, stack_elt_t*** stack_p, int* top_p, int* size_p);
tour_t* Dup_tour(tour_t* tour_p);
void Print_tour(tour_t* tour_p, char* title);

/*------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
   FILE*  mat_file;
   pthread_t* thread_handles;
   long thread;

   if (argc != 3) Usage(argv[0]);
   mat_file = fopen(argv[2], "r");
   if (mat_file == NULL) {
      fprintf(stderr, "Can't open %s\n", argv[1]);
      Usage(argv[0]);
   }
   Read_mat(mat_file);
   fclose(mat_file);
   thread_count = strtol(argv[1], NULL, 10);
   if (thread_count < 1 || n <= thread_count) Usage(argv[0]);

   thread_handles = malloc(thread_count * sizeof(pthread_t));
   pthread_mutex_init(&mutex, NULL);  

   Initialize_tour(&best_tour);
   best_tour.cost = INFINITY;

   for (thread = 0; thread < thread_count; thread++)
      pthread_create(&thread_handles[thread], NULL, Search, (void*) thread);

   for (thread = 0; thread < thread_count; thread++)
      pthread_join(thread_handles[thread], NULL);
   free(thread_handles);
   pthread_mutex_destroy(&mutex);
   free(mat);
   
   Print_tour(&best_tour, "Best tour");
   printf("Cost = %d\n", best_tour.cost);
   free(best_tour.cities);
   return 0;
}  /* main */

/*------------------------------------------------------------------
 * Function:  Usage
 * Purpose:   Inform user how to start program and exit
 * In arg:    prog_name
 */
void Usage(char* prog_name) {
   fprintf(stderr, "usage: %s <thread_count> <matrix file>\n", prog_name);
   fprintf(stderr, "thread_count must be  greater than or equal to 1\n");
   fprintf(stderr, "and less than number of cities\n");
   exit(0);
}  /* Usage */

/*------------------------------------------------------------------
 * Function:         Read_mat
 * Purpose:          Read in the number of cities and the matrix of costs
 * In arg:           mat_file
 * Global vars out:  mat, n
 */
void Read_mat(FILE* mat_file) {
   int i, j;

   fscanf(mat_file, "%d", &n);
   mat = malloc(n*n*sizeof(weight_t));

   for (i = 0; i < n; i++)
      for (j = 0; j < n; j++)
         fscanf(mat_file, "%d", &mat[n*i+j]);
}  /* Read_mat */

/*------------------------------------------------------------------
 * Function:    Initialize_tour
 * Purpose:     Initialize a tour with no visited cities
 * In/out arg:  tour_p
 */
void Initialize_tour(tour_t* tour_p) {
   int i;

   tour_p->cities = malloc((n+1)*sizeof(city_t));
   for (i = 0; i <= n; i++) {
      tour_p->cities[i] = NO_CITY;
   }
   tour_p->cost = 0;
   tour_p->count = 0;
}  /* Initialize_tour */

/*------------------------------------------------------------------
 * Function:            Search
 * Purpose:             Search for an optimal tour
 * Global vars in:      mat, n, mutex
 * Global vars in/out:  best_tour
 * Variables in:        rank
 */
void* Search(void* rank) {
   
   long my_rank = (long) rank;
   city_t nbr, city;
   weight_t cost;
   tour_t* temp_tour_p;
   tour_t local_best;
   local_best.cost = INFINITY;
   stack_elt_t** stack = malloc((n * 2) * sizeof(stack_elt_t*));
   int size = (n * 2), top = 0;
   
   Fill_stack(&stack, &top, my_rank, temp_tour_p);
   
   while (!Empty(&top)) {
      Pop(&temp_tour_p, &city, &cost, &stack, &top);
      temp_tour_p->cities[temp_tour_p->count] = city;
      temp_tour_p->cost += cost;
      temp_tour_p->count++;
      if (temp_tour_p->count == n) {
         pthread_mutex_lock(&mutex);
         Check_best_tour(city, temp_tour_p, &local_best);
         pthread_mutex_unlock(&mutex);
      } else {
         for (nbr = n-1; nbr > 0; nbr--) {
            if (Feasible(city, nbr, temp_tour_p, &local_best)) {
               Push(temp_tour_p, nbr, mat[n*city+nbr], &stack, &top, &size);
            }
         }
      }
      /* Push duplicates the tour.  So it needs to be freed */
      free(temp_tour_p->cities);
      free(temp_tour_p);
   }  /* while */
   free(stack);
   return NULL;
}  /* Search */

/*------------------------------------------------------------------
 * Function:        Fill_stack
 * Purpose:         Fill stack with designated two city partial tours
 * Global vars in:  thread_count, mat, n
 * In args:         my_rank: rank of thread
 * In/out args:     stack_p: pointer to stack                 
 *                  top_p: pointer to int that designates top of stack
 *                  temp_tour_p: pointer to temporary tour
 */
void Fill_stack(stack_elt_t*** stack_p, int* top_p, long my_rank, tour_t* temp_tour_p) {

   int quotient = (n-1)/thread_count, remainder = (n-1) % thread_count;
   int partial_tour_count, first_final_city, last_final_city, i;
   if (my_rank < remainder) {
      partial_tour_count = quotient + 1;
      first_final_city = my_rank * partial_tour_count + 1;
   } else {
      partial_tour_count = quotient;
      first_final_city = my_rank * partial_tour_count + remainder + 1;
   }
   last_final_city = first_final_city + partial_tour_count - 1;
   stack_elt_t** stack = *stack_p;
   
   for (i = first_final_city; i <= last_final_city; i++) {
      temp_tour_p = malloc(sizeof(tour_t));
      Initialize_tour(temp_tour_p);
      temp_tour_p->cities[0] = 0;
      temp_tour_p->count = 1;
      stack[*top_p] = malloc(sizeof(stack_elt_t));
      stack[*top_p]->tour_p = temp_tour_p;
      stack[*top_p]->city = i;
      stack[*top_p]->cost += mat[i];
      ++*top_p;
   }
}  /* Fill_stack */

/*------------------------------------------------------------------
 * Function:  Empty
 * Purpose:   Determine whether the stack is empty
 * In arg:    top_p: pointer to int that designates top of stack
 * Ret val:   TRUE if stack is empty, FALSE otherwise
 */
int Empty(int* top_p) {

   if (*top_p == 0)
      return TRUE;
   else
      return FALSE;
}  /* Empty */

/*------------------------------------------------------------------
 * Function:    Pop
 * Purpose:     Remove the top node from the stack and return it
 * In/out arg:  stack_p:  on input the current stack, on output
 *                 the stack with the top record removed
 *              top_p: pointer to int that designates top of stack
 * Out args:    tour_pp:  the tour in the top stack node
 *              city_p:   the city in the top stack node
 *              cost_p:   the cost of visiting the city
 */
void Pop(tour_t** tour_pp, city_t* city_p, weight_t* cost_p, stack_elt_t*** stack_p, int* top_p) {

   stack_elt_t** stack = *stack_p;
   *tour_pp = stack[--*top_p]->tour_p;
   *city_p = stack[*top_p]->city;
   *cost_p = stack[*top_p]->cost;
   free(stack[*top_p]);
}  /* Pop */

/*------------------------------------------------------------------
 * Function:            Check_best_tour
 * Purpose:             Determine whether the current n-city tour will be
 *                      better than the current best tour.  If so, update
 *                      best_tour and local_best_p
 * In args:             city, tour_p 
 * In/out args:         local_best_p
 * Global vars in:      mat, n
 * Global vars in/out:  best_tour
 */
void Check_best_tour(city_t city, tour_t* tour_p, tour_t* local_best_p) {

   int i;

   if (tour_p->cost + mat[city*n + 0] < best_tour.cost) {
      for (i = 0; i < tour_p->count; i++) {
         best_tour.cities[i] = tour_p->cities[i];
      }
      best_tour.cities[n] = 0;
      best_tour.count = n+1;
      best_tour.cost = tour_p->cost + mat[city*n + 0];
      local_best_p->cost = best_tour.cost;
   }
}  /* Check_best_tour */

/*------------------------------------------------------------------
 * Function:        Feasible
 * Purpose:         Check whether nbr could possibly lead to a better
 *                  solution if it is added to the current tour.  The
 *                  functions checks whether nbr has already been visited
 *                  in the current tour, and, if not, whether adding the
 *                  edge from the current city to nbr will result in
 *                  a cost less than the current best cost.
 * In args:         All
 * Global vars in:  mat, n, best_tour
 * Return:          TRUE if the nbr can be added to the current tour.
 *                  FALSE otherwise
 */
int Feasible(city_t city, city_t nbr, tour_t* tour_p, tour_t* local_best_p) {
   if (!Visited(nbr, tour_p) && (tour_p->cost + mat[n*city + nbr] < local_best_p->cost)) 
      return TRUE;
   else
      return FALSE;
}  /* Feasible */

/*------------------------------------------------------------------
 * Function:   Visited
 * Purpose:    Use linear search to determine whether nbr has already
 *             bee visited on the current tour.
 * In args:    All
 * Return val: TRUE if nbr has already been visited.  
 *             FALSE otherwise
 */
int Visited(city_t nbr, tour_t* tour_p) {
   int i;

   for (i = 0; i < tour_p->count; i++)
      if ( tour_p->cities[i] == nbr ) return TRUE;
   return FALSE;
}  /* Visited */

/*------------------------------------------------------------------
 * Function:    Push
 * Purpose:     Add a new node to the top of the stack
 * In args:     tour_p, city, cost
 * In/out arg:  stack_p: on input pointer to current stack
 *              on output pointer to stack with new top record
 *              top_p: pointer to int that designates top of stack
 *              size_p: pointer to int that designates size of stack array
 * Note:        The input tour is duplicated before being pushed
 *              so that the existing tour can be used in the 
 *              Search function
 */
void Push(tour_t* tour_p, city_t city, weight_t cost, stack_elt_t*** stack_p, int* top_p, int* size_p) {

   if (*top_p == *size_p) {
      *stack_p = realloc(*stack_p, *size_p * 2 * sizeof(stack_elt_t*));
      *size_p = *size_p * 2;
   }
   stack_elt_t** stack = *stack_p;
   stack[*top_p] = malloc(sizeof(stack_elt_t));
   stack[*top_p]->tour_p = Dup_tour(tour_p);
   stack[*top_p]->city = city;
   stack[*top_p]->cost = cost;
   ++*top_p;
}  /* Push */

/*------------------------------------------------------------------
 * Function:  Dup_tour
 * Purpose:   Create a duplicate of the tour referenced by tour_p:
 *            used by the Push function
 * In arg:    tour_p
 * Ret val:   Pointer to the copy of the tour
 */
tour_t* Dup_tour(tour_t* tour_p) {

   int i;
   tour_t* temp_p = malloc(sizeof(tour_t));
   temp_p->cities = malloc(n*sizeof(city_t));
   for (i = 0; i < n; i++)
      temp_p->cities[i] = tour_p->cities[i];
   temp_p->cost = tour_p->cost;
   temp_p->count = tour_p->count;
   return temp_p;
}  /* Dup_tour */

/*------------------------------------------------------------------
 * Function:  Print_tour
 * Purpose:   Print a tour
 * In args:   All
 */
void Print_tour(tour_t* tour_p, char* title) {
   int i;

   printf("%s:\n", title);
   for (i = 0; i < tour_p->count; i++)
      printf("%d ", tour_p->cities[i]);
   printf("\n\n");
}  /* Print_tour */
