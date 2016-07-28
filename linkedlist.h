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

#ifndef LINKED_LIST_H
#define LINKED_LIST_H



#define OCI_CHECK(exp, ret) if ((exp) == TRUE) return (ret);

struct ListNode {
   void              *data; /* pointer to data */
   struct ListNode   *next; /* next pointer */
} ;

typedef struct ListNode ListNode;


struct LinkedList {
    ListNode    *head;   /* head of the list */   
    int          ncount; /* number of nodes in the list */ 
} ;

typedef struct LinkedList  LinkedList;


LinkedList * ListCreate();

void ListFree( LinkedList *list  );

ListNode * ListCreateNode(int size );

ListNode * ListAppend( LinkedList *list, int size  );

void ListRemove(  LinkedList *list, void *data  );   



#endif 
