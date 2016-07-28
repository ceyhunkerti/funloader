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

#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#define _GNU_SOURCE
#include <time.h>


char *get_local_time( void );
char *get_strftime ( char * format );
long get_seconds( const char * timestr,const char *time_format);
extern int select();

#ifdef FUL_HAVE_USLEEP
int usleep( long useconds );    
#endif

#endif
