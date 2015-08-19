/* File:     linked_list.c
 *
 * Purpose:  Implement a sorted linked list of strings with ops insert,
 *           print, member, delete, free_list.
 * 
 * Input:    Single character lower case letters to indicate operators, 
 *           followed by arguments needed by operators.
 * Output:   Results of operations.
 *
 * Compile:  gcc -g -Wall -o linked_list linked_list.c
 * Run:      ./linked_list
 *
 * Notes:
 *    1.  Repeated values aren't added to the list
 *    3.  Program assumes a string will be entered when prompted
 *        for one.
 *
 * Name:    Joseph Tanigawa
 * Date:    9/30/2012
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>       

struct list_node_s {
   char*  data;
   struct list_node_s* next_p;
};

int Member(struct list_node_s* head_p, char val[]);
struct list_node_s* Delete(struct list_node_s* head_p, char val[]);
struct list_node_s* Allocate_node(char** val);
void Free_node(struct list_node_s* node_p);
struct list_node_s* Insert(struct list_node_s* head_p, char val[]);
void Print(struct list_node_s* head_p);
struct list_node_s* Free_list(struct list_node_s* head_p); 
char Get_command(void);
void Get_value(char val[]);

/*-----------------------------------------------------------------*/
int main(void) {
   char command, value[100];
   struct list_node_s* head_p = NULL;  
      /* start with empty list */
   command = Get_command();
   
   while (command != 'q' && command != 'Q') {
   
      switch (command) {
         case 'i': 
         case 'I': 
            Get_value(value);
            head_p = Insert(head_p, value);
            break;
         case 'p':
         case 'P':
            Print(head_p);
            break;
         case 'm': 
         case 'M':
            Get_value(value);
            if (Member(head_p, value))
               printf("%s is in the list\n", value);
            else
               printf("%s is not in the list\n", value);
            break;
         case 'd':
         case 'D':
            Get_value(value);
            head_p = Delete(head_p, value);
            break;
         case 'f':
         case 'F':
            head_p = Free_list(head_p);
            break;
         default:
            printf("There is no %c command\n", command);
            printf("Please try again\n");
      }
      command = Get_command();
   }
   head_p = Free_list(head_p);
   return 0;
}  /* main */

/*-----------------------------------------------------------------
 * Function:    Member
 * Purpose:     search list for val
 * Input args:  head_p:  pointer to head of list
 *              val:  value to search for
 * Return val:  1 if val is in list, 0 otherwise
 */
int Member(struct list_node_s* head_p, char val[]) {

   struct list_node_s* curr_p = head_p;

   while (curr_p != NULL) {
   
      if (strcmp(curr_p->data, val) == 0) {
         
         return 1;
      }else if (strcmp(val, curr_p->data) < 0)  {
         
         return 0;  
      }else {
      
         curr_p = curr_p->next_p;
      }
   }
   return 0;
}  /* Member */

/*-----------------------------------------------------------------
 * Function:   Delete
 * Purpose:    Delete the first occurrence of val from list
 * Input args: head_p: pointer to the head of the list
 *             val:    value to be deleted
 * Return val: Possibly updated pointer to head of list
 */
struct list_node_s* Delete(struct list_node_s* head_p, char val[]) {
   struct list_node_s* curr_p = head_p;
   struct list_node_s* pred_p = NULL;  /* Points to predecessor of curr_p or
                                        * NULL when curr_p is first node */
   /* Find node containing val and predecessor of this node */
   while (curr_p != NULL) {
   
      if (strcmp(curr_p->data, val) == 0) {
      
         break;
      }else if (strcmp(val, curr_p->data) < 0) {

         printf("%s isn't in the list\n", val);
         return head_p;  
      }else { // curr_p->data != val 
      
         pred_p = curr_p;
         curr_p = curr_p->next_p;
      }
   }
   
   if (curr_p == NULL) {
   
      printf("%s isn't in the list\n", val);
   }else {
   
      if (pred_p == NULL) {  /* val is in first node */
      
         head_p = curr_p->next_p;
         Free_node(curr_p);
      }else { /* val not in first node */
      
         pred_p->next_p = curr_p->next_p;
         Free_node(curr_p);
      }
   } 
   return head_p;
}  /* Delete */

/*-----------------------------------------------------------------
 * Function:   Allocate_node
 * Purpose:    Create list_node_s with Data pointing to the
 *             copy of val char array
 * Input args: val: string to be copied effieciently, Data in the 
               list_node_s will then point to the copy of val.
 * Return val: Pointer to just created list_node_s
 */
struct list_node_s* Allocate_node(char** val) {

   struct list_node_s* temp_p = malloc(sizeof(struct list_node_s));
   char* exact_string_p = malloc((strlen(*val) + 1) * sizeof(char));
   exact_string_p = strcpy(exact_string_p, *val);
   temp_p->data = exact_string_p;   
   return temp_p;
}  /* Allocate_node */

/*-----------------------------------------------------------------
 * Function:   Free_node
 * Purpose:    Delete list_node_s along with 
 *             char array that Data points to
 * Input args: node_p: pointer to node that will be deleted
 */
void Free_node(struct list_node_s* node_p) {

   free(node_p->data);
   free(node_p);
}  /* Free_node */

/*-----------------------------------------------------------------
 * Function:   Insert
 * Purpose:    Insert val in the corect position for the sorted list
 * Input args: head_p: pointer to head of list
 *             val:  new value to be inserted
 * Return val: Pointer to head of list
 */
struct list_node_s* Insert(struct list_node_s* head_p, char val[]) {
   
   if (head_p == NULL) {   /* inserting into empty list */
   
      head_p = Allocate_node(&val);
      return head_p;
   }
   struct list_node_s* curr_p = head_p;
   struct list_node_s* pred_p = NULL;
   
   while ((curr_p->next_p != NULL) && ((strcmp(val, curr_p->data) > 0))) {
   
      pred_p = curr_p;
      curr_p = curr_p->next_p;   
   }
   
   if (strcmp(val, curr_p->data) == 0) { /* val is already in the list */
   
      return head_p;
   }
   struct list_node_s* temp_p = Allocate_node(&val);
   
   if (strcmp(val, curr_p->data) > 0) {   /*inserting at the end of the list */
   
      temp_p->next_p = NULL;
      curr_p->next_p = temp_p;     
   }else if (pred_p == NULL) {   /*inserting at the beginning of the list */
   
      temp_p->next_p = curr_p;
      head_p = temp_p;  
   }else {  /*inserting in the middle of the list */
   
      temp_p->next_p = curr_p;
      pred_p->next_p = temp_p;  
   } 
   return head_p;
}  /* Insert */
 
/*-----------------------------------------------------------------
 * Function:   Print
 * Purpose:    Print list on a single line of stdout
 * Input arg:  head_p
 */
void Print(struct list_node_s* head_p) {

   struct list_node_s* curr_p = head_p;
   printf("list = ");
   
   while (curr_p != NULL) {
   
      printf("%s ", curr_p->data);
      curr_p = curr_p->next_p;
   }  
   printf("\n");
}  /* Print */

/*-----------------------------------------------------------------
 * Function:    Free_list
 * Purpose:     free each node in the list
 * Input arg:   head_p:  pointer to head of list
 * Return val:  NULL pointer
 * Note:        head_p is set to NULL on completion, indicating
 *              list is empty.
 */
struct list_node_s* Free_list(struct list_node_s* head_p) {

   struct list_node_s* curr_p;
   struct list_node_s* temp_p;
   curr_p = head_p;
   
   while (curr_p != NULL) {
   
      temp_p = curr_p;
      curr_p = curr_p->next_p;
      Free_node(temp_p);    
   }
   head_p = NULL;
   return head_p;
}  /* Free_list */

/*-----------------------------------------------------------------
 * Function:      Get_command
 * Purpose:       Get a single character command from stdin
 * Return value:  the first non-whitespace character from stdin
 */
char Get_command(void) {

   char c;
   printf("Please enter a command (i, p, m, d, f, q):  ");
   /* Put the space before the %c so scanf will skip white space */
   scanf(" %c", &c);
   return c;
}  /* Get_command */

/*-----------------------------------------------------------------
 * Function:   Get_value
 * Purpose:    Get a string from stdin and store it in val
 */
void Get_value(char val[]) {

   printf("Please enter a value:  ");
   scanf("%s", val);
}  /* Get_value */
