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
#include <stdlib.h>


/* ------------------------------------------------- *
 * prints error message and terminates program if a  * 
 * severe error occures                              * 
 * ------------------------------------------------- */ 
void fatal(char *fstr )
{
	fprintf(stderr,"Fatal error occured !\n"
			"Error : %s\n"
			"TERMINATING\n",fstr);
	exit(1);

}

/* ------------------------------------------------- *
 * calls malloc and handles insufficient and         * 
 * negative memory value exceptions. By modifing     *
 * this function you can also inspect memory leaks   *
 * ------------------------------------------------- */ 
void *safe_malloc(size_t size)
{
   void *tmp_mem;
	
#ifdef FUL_DEBUG_MALLOC_SIZE
   printf("malloc size : %zu\n",size);
#endif
   if ((int) size < 0)  
      fatal("Can not allocate negative size memory !\n");
  
   tmp_mem = malloc(size);
  
   if (tmp_mem == NULL)
      fatal("Failed to allocate memory ! \n");

   return tmp_mem;
}

/* ------------------------------------------------- *
 * returns true if given string is a number          *
 * returns false if the string contains              *
 * nondigit characters                               *
 * ------------------------------------------------- */ 
bool isNumber(char *str) {

   unsigned int i ;
   
   for( i = 0; *(str+i); i++ ) 
      if(!isdigit(*(str+i)))
         return 0;

   return 1;
}
