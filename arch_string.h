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


#ifndef ARCH_STRING_H
#define ARCH_STRING_H


#include <string.h>
#include <ctype.h>

/* --------------------------------------------------------
 * this routine is an asm implentation of string lib's 
 * strcpy this one is faster and this one should be used 
 * in big loops instead of strcpy. This function is unsafe 
 * like strcpy of string.h and should be used 
 * carefully
 * ------------------------------------------------------- */
#define HAVE_ASM_STRCPY
extern inline char * strcpy(char * dest, const char *src)
{
   int d0, d1, d2;
   __asm__ __volatile__(  "1:\tlodsb\n\t"
                          "stosb\n\t"
                          "testb %%al,%%al\n\t"
                          "jne 1b"
                        : "=&S" (d0), "=&D" (d1), "=&a" (d2)
                        : "0" (src),"1" (dest) 
                        : "memory");
return dest;
}

/* --------------------------------------------------------
 * this routine is an asm implentation of string lib's 
 * strcat this one is faster and this one should be used 
 * in big loops instead of strcat. This function is unsafe 
 * like strcpy of string.h and should be used 
 * carefully
 * ------------------------------------------------------- */
#define HAVE_ASM_STRCAT
extern inline char *strcat(char * dest, const char * src)
{
    int d0, d1, d2, d3;
   
    __asm__ __volatile__(  
            "repne\n\t"
            "scasb\n\t"
            "decl %1\n"
            "1:\tlodsb\n\t"
            "stosb\n\t"
            "testb %%al,%%al\n\t"
            "jne 1b"
            : "=&S" (d0), "=&D" (d1), "=&a" (d2), "=&c" (d3)
            : "0" (src), "1" (dest), "2" (0), "3" (0xffffffff):"memory");

    return dest;
}

/* ----------------------------------------- *
 * Assembly version of strlen function       *
 * ----------------------------------------- */
#define HAVE_ASM_STRLEN
extern inline size_t strlen(const char *s)
{
    int d0;
    register int __res;

    __asm__ __volatile__(  
            "repne\n\t"
            "scasb\n\t"
            "notl %0\n\t"
            "decl %0"
            :"=c" (__res), "=&D" (d0) :"1" (s),"a" (0), "0" (0xffffffff));

    return __res;
}


/* ----------------------------------------- *
 * Inline assembly version of memset function*
 * ----------------------------------------- */
#define HAVE_ASM_MEMSET
extern inline void *memset(void *s, int c, size_t count)
{
    int d0, d1;

    __asm__ __volatile__(
	         "rep\n\t"
	         "stosb"
	         : "=&c" (d0), "=&D" (d1)
	         :"a" (c),"1" (s),"0" (count)
	         :"memory");

    return s;
}

/* ----------------------------------------- *
 * Inline assembly version of memset function*
 * ----------------------------------------- */
#define HAVE_ASM_MEMCPY
extern inline void *memcpy(void * to, const void * from, size_t n)
{
    int d0, d1, d2;

    __asm__ __volatile__(
	         "rep ; movsl\n\t"
	         "testb $2,%b4\n\t"
	         "je 1f\n\t"
	         "movsw\n"
	         "1:\ttestb $1,%b4\n\t"
	         "je 2f\n\t"
	         "movsb\n"
	         "2:"
	         : "=&c" (d0), "=&D" (d1), "=&S" (d2)
	         :"0" (n/4), "q" (n),"1" ((long) to),"2" ((long) from)
	         : "memory");

    return (to);
}


#endif
