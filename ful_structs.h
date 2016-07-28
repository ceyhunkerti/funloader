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


#ifndef FUL_STRUCTS_H
#define FUL_STRUCTS_H

/* rowid information */
struct rowid_info
{
   unsigned short int rowid_range_count;
   char **rowid_from;   /* rowid range begin */
   char **rowid_to;     /* rowid range end */
} ;

typedef struct rowid_info rowid_info;

/* ------------------------------------
 * this structure contains logs about
 * execution.
 * ------------------------------------ */
struct ul_logs
{
   char *start_time;                /* unload start time             */
   char *end_time;                  /* unload end time               */ 
   unsigned long int elapsed_time;  /* elapsed time in seconds       */
   unsigned long int serial_elapsed_time; /* elapsed time in seconds */
   unsigned long int row_count;           /* number of unloaded rows */ 
} ;


typedef struct ul_logs ul_logs;

/* info about partition */
struct partition
{
   char *name;             /* partition name    */
   rowid_info *rowidinfo;  /* rowid information */
   ul_logs *ulog;          /* unload logs       */
} ;

typedef struct partition partition;


/* info about subpartition */
struct subpartition
{
   char *name;             /* subpartition name */
   rowid_info  *rowidinfo; /* rowid information */
   ul_logs *ulog;          /* unload logs       */
} ;

typedef struct subpartition subpartition;

/* ------------------------------------
 * this structure contains information
 * about main table in the given SQL
 * statement. Parallel executions are
 * done using the main table information.
 * ------------------------------------ */
struct main_table
{
   char *name;                    /* main table schema         */
   char *owner;                   /* main table name           */
   char *allias;                  /* allias of the main table  */ 
   unsigned short int part_level; /* partition level  */
   LinkedList *part_list;     /* partition info list  */
   LinkedList *subpart_list;  /* subpartition list    */
   rowid_info *rowidinfo;     /* rowid information    */
} ;

typedef struct main_table main_table;


/* ------------------------------------
 * this structure contains helper 
 * varaibles to control the execution
 * flow. values those variables 
 * are either directly derived from the 
 * user options or calculated using
 * user options and metadata information.
 * ------------------------------------ */
struct unload_variables
{
   OCI_Mutex *mtx;
   unsigned int con_ses_mode;
   unsigned short int par_degree;
   unsigned short int par_type;
   char *field_delimiter;
   char *record_delimiter;   
   unsigned int fetch_size;
   ul_logs ulog;         /* unload logs                  */
   ul_logs llog;         /* load logs                    */  
   bool set_partlist;    /* set partition name list      */
   bool set_subpartlist; /* set subpartition name list   */
   unsigned short int unloadstream_type;   
   /* ... these variables used in query rewrite ... */
   char *query_part_pos;
   bool query_has_where;   

} ;

typedef struct unload_variables unload_variables;


/* ------------------------------------
 * this structure contains unload data
 * for the unloading thread(s).
 * thread data structure contains both
 * input and aoutput variables 
 * ------------------------------------ */
struct unload_thread_data
{ 
   OCI_Thread *thread_id;        /* internal thread id */
   unsigned short int thread_no; /* sequential thread number */
   char *unload_query;           /* unload query */
   ul_logs *ulog;                /* unload logs  */
   char *unloadfile;             /* unlaod file name */  

} ;

typedef struct unload_thread_data unload_thread_data;

/* ------------------------------------
 * this structure contains load data
 * for the loading thread(s).
 * thread data structure contains both
 * input and aoutput variables 
 * ------------------------------------ */
struct load_thread_data
{ 
   OCI_Thread *thread_id;              /* internal thread id */
   unsigned short int thread_no;       /* sequential thread number */
   unsigned short int sqlldr_ret_code; /* sql loader return code */
   ul_logs *llog;                      /* unload logs  */
   char *loadfile;                     /* unlaod file name */ 
   char *sqlldr_cmd;
} ;

typedef struct load_thread_data load_thread_data;

/* ------------------------------------
 * this structure contains the
 * attributes of a column. only needed
 * attributes exist in here
 * ------------------------------------ */
struct column_info
{
   char *name;
   char *sqltype;
   bool charused;
   unsigned int size;
} ;

typedef struct column_info column_info;
/* ------------------------------------
 * record_info structure contains 
 * metadata about a record.
 * ------------------------------------ */
struct record_info
{
   column_info **col;         /* column structure array */
   unsigned short int col_cnt;/* number of columns in the sql statement */   
   unsigned int max_rec_len;  /* total length of all columns that appears in the sql statement */  
} ;

typedef struct record_info record_info;

/* ------------------------------------
 * unloadquery_metadata structure 
 * contains metadata about the unload
 * query taht is given by the user.
 * ------------------------------------ */
struct unloadquery_metadata
{ 
   char *unloadquery;
   record_info *record;
   main_table *mt;            /* main table structure */
} ;

typedef struct unloadquery_metadata unloadquery_metadata;

/* ------------------------------------
 * options structure contains the 
 * variables that is supplied by the
 * user. those variables could be 
 * stored in their raw forms or could 
 * be stored in converted forms. 
 * ------------------------------------ */
struct options 
{
   bool debugging; 
   int  log_level;
   char *log_file;
   char *unload_query;
   char *unload_query_file;
   char *config_file;
   char *unload_file;         /* unload file name        */
   char *net_service_name;    /* net service name        */
   char *con_ses_mode; /* connect session mode */
   char *user_name;
   char *password;
   /* Main Table */
   char *mtab_owner;
   char *mtab_name;
   char *mtab_part_level;
   char *mtab_allias;
   /* Delimiters */
   char *xfield_delimiter;    /* column separator hex */
   char *xrecord_delimiter;   /* record separator hex */
   char *field_delimiter;  
   char *record_delimiter; 
   
   char *partlist;            /* partition list string */
   char *subpartlist;         /* subpartition list string */   
   
   char *date_format;         /* used for date string conversion */

   char *fetch_size;          /* fetch size     */
   int  w_buffer_size;        /* write buffer size in records*/
   char *par_degree;          /* parallel degree         */   
   char *par_type;            /* parallelisation type    */     
   bool unload2pipe;          /*nload to pipe          */ 

   bool load_data;            /* load data to target table */
   bool gen_ctl_file;         /* generate ctl file */
   char *sqlldr_cmd;          /* sql loader command used when calling sqlldr */
   char *target_table_name;   /* name of the target table */
   char *target_table_owner;  /* schema of the target table */
   bool truncate_target_table;/* truncate target table */
   char *ctl_file_name;       /* user supplied ctl file name */


} ;

typedef struct options options ;


#endif
