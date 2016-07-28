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

#include "str_utils.h"


/* ------------------------------------------------- *
 * returns a given string with no spaces in the      *
 * end and beginnig                                  * 
 * ------------------------------------------------- */
char * trim(char *s) {

	int i=0,j;

   if(s == NULL) return s;

	while((s[i]==' ')||(s[i]=='\t')) {
		i++;
	}
	if(i>0) {
		for(j=0;j<strlen(s);j++) s[j]=s[j+i];
		
	   s[j]='\0';
	}

	i = strlen(s) - 1;
	while((s[i]==' ')||(s[i]=='\t')) i--;

	if(i<(strlen(s)-1)) {
		s[i+1]='\0';
	}
   return s;
}

/* ------------------------------------------------- *
 * trims the rightmost given characters of the       *
 * given string                                      *
 * ------------------------------------------------- */
char * rtrim( char * string, char * trim ) {
	
   int i;
   
   if(string == NULL) return string;

	for( i = strlen (string) - 1; i >= 0 
	   && strchr ( trim, string[i] ) != NULL; i-- )
      string[i] = '\0';
  
   return string;
}

/* ------------------------------------------------- *
 * trims the leftmost given characters of the        *
 * given string                                      *
 * ------------------------------------------------- */
char * ltrim( char * string, char * trim ) {
   
   if(string == NULL) return string;

	while ( string[0] != '\0' && strchr ( trim, string[0] ) != NULL )
		memmove( &string[0], &string[1], strlen(string) );

   return string;
}

/* ------------------------------------------------- *
 * converts a string to uppercase                    *
 * ------------------------------------------------- */
char *strupper(char **str)
{
   char *tmpstr = *str;

      if (tmpstr)
      {
         for ( ; *tmpstr; tmpstr++)
            *tmpstr = toupper(*tmpstr);
      }
 
      return *str;
}

/* ------------------------------------------------- *
 * converts a string to lowercase                    *
 * ------------------------------------------------- */
char *strlower(char **str)
{
   char *tmpstr = *str;

   if (*str)
   {
      for ( ; *tmpstr; tmpstr++)
         *tmpstr = tolower(*tmpstr);
   }
   
   return *str;
}

/* ------------------------------------------------- *
 * insert a string to the given                      *
 * posion of another string                          *
 *                                                   *
 * PARAMETERS                                        *
 * ----------                                        *
 * str      : destination string                     *
 * s        : string to be inserted                  *
 * position : position in str which s will be        *
 *            inserted                               *  
 * EXAMPLE                                           *
 * ---------                                         *
 * $ char *s = strinsert("floader",1,"un")           *
 * $ print s                                         *
 * will print funloader                              *  
 * ------------------------------------------------- */
char *strinsert(const char *str, const char *position, const char *s)
{
   char *tmpstr;   
   int sz01,sz02;


   if(!s||!position) return (char *)str;

   
   sz01 = strlen(str);
   sz02 = (s!=NULL || *s != '\0') ? strlen(s): 0 ;

   tmpstr = (char *)safe_malloc( sz01 + sz02 +1);
 
   memset(tmpstr, '\0',sz01 + sz02 +1); 

   /* copy memory contents untill position */
   strncpy(tmpstr,str,sz01-strlen(position));
   
   /* append insert string */
   strcat(tmpstr,s);

  
   /* append the rest of the string */
   if(position && *position != '\0')
      strcat(tmpstr,position);
      
   
   return tmpstr;
   
}

