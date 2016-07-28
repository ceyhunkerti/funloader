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


#include "time_utils.h"
#include "utils.h"
#include "str_utils.h"
#define _GNU_SOURCE

/* ------------------------------------------------- *
 * returns current time as string in                 *
 * "%d %b %Y %H:%M:%S" format                        *
 * ------------------------------------------------- */ 
char * get_local_time( void )
{
	time_t rawtime;
   struct tm * timeinfo;   

	time(&rawtime);
   timeinfo = localtime ( &rawtime );   

   return rtrim(asctime(timeinfo), "\n");

}

/* ------------------------------------------------- *
 * returns current time as string in given format    *
 * ------------------------------------------------- */
char * get_strftime( char * format)
{
   char *outstr;
   time_t t;
   struct tm *tmp;
    
   time(&t);
   tmp = localtime(&t);
    
   outstr = (char *)safe_malloc(50*sizeof(char));

   if (tmp == NULL) 
      return (char *)NULL;


   strftime(outstr, 50, format , tmp);
    
    return outstr;

} 
   
/* ------------------------------------------------- *
 * returns elapsed number of seconds since from the  *
 * epoc time using input time string and time format * 
 * ------------------------------------------------- */
long get_seconds( const char * timestr,const char *time_format)
{
   long seconds;
   struct tm tm;
   
   if (strptime(timestr, time_format , &tm) == NULL)
      fatal("Unable to convert string to time object\n\n");  

   if( (seconds = mktime(&tm)) == -1)
      fatal("Unable to convert time object to seconds\n\n");

   return seconds;

}

#ifdef FUL_HAVE_USLEEP
/* ------------------------------------------------- *
 * usleep is not a standard library function         *
 * so it is better to imlement it in here as a BSD   *
 * system call emulation. this function sleeps for   *
 * given amount of microseconds.                     * 
 * ------------------------------------------------- */
int usleep( long useconds )                            
{
   
   static struct timeval tv;

   tv.tv_sec  = useconds / 1000000L;
   tv.tv_usec = useconds % 1000000L;

   return select( 0, NULL, NULL, NULL, &tv );
}
#endif
