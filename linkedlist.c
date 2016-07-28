/*
    +----------------------------------------------------------------------+   
    |                                                                      |
    |                  FUNLOADER - Fast Unloader-Loader                    |
    |                                                                      |
    |                (Fast Unload/Load Tool with OCI)                      |
    |                                                                      |
    +----------------------------------------------------------------------+
    |                      Website : xxxxxxxxxxxxxxxxx                     |
    +----------------------------------------------------------------------+
    |               Copyright (c) 2009-2010 Ceyhun Kerti                   |
    +----------------------------------------------------------------------+
    | This program is free software; you can redistribute it and/or        |
    | modify it under the terms of the GNU Library General Public          |
    | License as published by the Free Software Foundation; either         |
    | version 2 of the License, or (at your option) any later version.     |
    |                                                                      |
    | This program is distributed in the hope that it will be useful,      |
    | but WITHOUT ANY WARRANTY; without even the implied warranty of       |
    | MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    |
    | Library General Public License for more details.                     |
    |                                                                      |
    | You should have received a copy of the GNU Library General Public    |
    | License along with this library; if not, write to the Free           |
    | Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.   |
    +----------------------------------------------------------------------+
    |            Author: Ceyhun Kerti <ceyhun.kerti@gmail.com>             |
    +----------------------------------------------------------------------+
*/


#include "utils.h"
#include "linkedlist.h"

/* TODO : this code should be fixed to handle multi threading */

/* ------------------------------------------------- *
 * creates a linked list object and returns a        *
 * pointer to it.                                    *
 * ------------------------------------------------- */
LinkedList * ListCreate()
{
     LinkedList *list = NULL;
      
     list = (LinkedList *) safe_malloc(sizeof(struct LinkedList));
 
     list->head   = NULL;
     list->ncount = 0; 

     return list;
}


/* ------------------------------------------------- *
 * creates a linked list node and return a pointer   *
 * to the newly allocated node                       *
 * ------------------------------------------------- */
ListNode * ListCreateNode(int size)
{
     ListNode *node  = NULL;
  
     node = (ListNode *) safe_malloc(sizeof(*node));
 
     node->data = (void *) safe_malloc( size );
     node->next = NULL;   

     return node;
 }


/* ------------------------------------------------- *
 * frees the linked list                             *
 * ------------------------------------------------- */
void ListFree(LinkedList *list)
{
 
   ListNode *node;
   ListNode *tmp;

   node = list->head;
   
   while(node != NULL ) {
      tmp  = node;
      node = node->next;
      free(tmp->data);
      free(tmp);
   }   
   list->head  = NULL;
   list->ncount = 0;


}

/* ------------------------------------------------- *
 * appends the given node to the end of the list     *
 * ------------------------------------------------- */
ListNode * ListAppend(LinkedList *list, int size)
{
   ListNode *node = NULL;
   ListNode *tmp = NULL;

   OCI_CHECK(list == NULL, NULL);
   
   node = ListCreateNode( size );
    
   OCI_CHECK(node == NULL, FALSE);
   
      
   tmp = list->head;
   
 
   while (tmp != NULL && tmp->next)
   {
      tmp = tmp->next;
   }
 
   if (tmp != NULL)
      tmp->next = node;
   else
      list->head = node;
   
   list->ncount++;

   return node;
}

/* ------------------------------------------------- *
 * removes the list node(s) with the given data      *
 * ------------------------------------------------- */
void ListRemove(LinkedList *list, void *data) {
   
   ListNode *node = NULL;
   ListNode *tmp = NULL;

   node = list->head;
 
   while (node != NULL)
   {
      if (node->data == data)
      {
         if (tmp) tmp->next = node->next;

         if (node == list->head) list->head = node->next;
         
         free(node);

         list->ncount--;
      }
      tmp = node;
      node = node->next;
   }

}

