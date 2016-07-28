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


#ifndef DEFS_H
#define DEFS_H


#define FUL_PROGRAM_NAME      "FUNLOADER"
#define FUL_PROGRAM_VERSION   "0.1.3"



#ifndef INLINE
# if __GNUC__
#  define _GNU_SOURCE   
#  define INLINE extern inline
# else
#  define INLINE inline
# endif
#endif



#ifndef bool
#define bool unsigned short int
#endif


#ifndef TRUE 
#define TRUE 1
#endif

#ifndef FALSE 
#define FALSE !TRUE
#endif



#define FUL_DS_TYPE_TABLE              0
#define FUL_DS_TYPE_PARTITION          1
#define FUL_DS_TYPE_SUBPARTITION       2

#define FUL_MAX_TABLE_NAME_LEN         30 
#define FUL_MAX_TABLE_ALLIAS_LEN       30
#define FUL_MAX_USER_NAME_LEN          30
#define FUL_MAX_PARTITION_NAME_LEN     30
#define FUL_MAX_SUBPARTITION_NAME_LEN  30


#define FUL_PARTITION_LEVEL_NONE         0 /* partition level none */
#define FUL_PARTITION_LEVEL_PARTITION    1 /* partition level partition */
#define FUL_PARTITION_LEVEL_SUBPARTITION 2 /* partition level subpartition */
#define FUL_PARTITION_LEVEL_UNKNOWN      3 /* partition level unknown */

#define FUL_PARALLEL_TYPE_NONE         0
#define FUL_PARALLEL_TYPE_PARTITION    1
#define FUL_PARALLEL_TYPE_SUBPARTITION 2
#define FUL_PARALLEL_TYPE_ROWID        3

#define FUL_DEFAULT_LOG_LEVEL 3

#define FUL_DEBUG_LOG         1  /* debugging log */
#define FUL_EXEC_LOG          2  /* execution log */  

#ifdef FUL_ENABLE_AUTO_FETCH_SIZE 
#  define FUL_MAGIC_RATIO 3/4
#endif


#define FUL_MAX_LOG_LEVEL     5
#define FUL_MIN_LOG_LEVEL     1
#define FUL_DEFAULT_LOG_LEVEL 3

#define FUL_DEFAULT_PREFETCH_SIZE   40
#define FUL_DEFAULT_FETCH_SIZE      40

#define FUL_LINE_BUFFER_SIZE  150
#define FUL_MAX_OPTION_LEN    35
#define FUL_MAX_OPTARG_LEN    4000

#define FUL_MAX_PARALLEL_DEGREE     128
#define FUL_DEFAULT_PARALLEL_DEGREE 8



#define FUL_RECORD_DELIMITER  0
#define FUL_FIELD_DELIMITER   1

#define FUL_UNLOAD_STREAM_TYPE_FILE   0
#define FUL_UNLOAD_STREAM_TYPE_PIPE   1
 

#define FUL_MIN_UQ_FILE_SIZE 16 /* SELECT * FROM a */

#define FUL_CLEAN_CODE_SUCCESS 0
#define FUL_CLEAN_CODE_FAILURE 1


/* #define FUL_ONE_SECOND 1000000  in micro seconds */
/* #define FUL_LOAD_DELAY FUL_ONE_SECOND/10 */

#define FUL_SQLLDR_EX_SUCC	0 
#define FUL_SQLLDR_EX_FAIL	1 
#define FUL_SQLLDR_EX_WARN	2 
#define FUL_SQLLDR_EX_FTL	3 
 
/* #define FUL_ENABLE_AUTO_FETCH_SIZE */

/* ---------- Detailed Debugging Controls ------------ */

/* 
 * this macros enables control over logging
 * for corresponding information. you can give them at compile
 * time with -D flag 
 */
   
/* 0  #define FUL_DEBUG_AGGRESSIVE		 	 includes 1,2,3,4 */ 
/* 1  #define FUL_DEBUG_UNLOAD_THREAD_DATA   */
/* 2  #define FUL_DEBUG_LOAD_THREAD_DATA     */
/* 3  #define FUL_DEBUG_SQLLDR_COMMAND       */
/* 4  #define FUL_DEBUG_ROWID_RANGES         */
/* 5  #define FUL_DEBUG_SYS_INFO             */
/* 6  #define FUL_DEBUG_DB_VERSION           */
/* 7  #define FUL_DEBUG_MALLOC_SIZE          */

/* ---------- Macro Definitions ----------------- */

#define MAX(a, b)  (((a) > (b)) ? (a) : (b))
#define MIN(a, b)  (((a) > (b)) ? (b) : (a))


#endif
