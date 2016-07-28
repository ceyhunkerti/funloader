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


/*----------------------------------------------------------------*/

/*
 * LD_LIBRARY_PATH=/usr/local/lib:$ORACLE_HOME/lib
 * gcc test2.c -g -O2 -O0 -o test INLINE ASM
 * 
 * gcc -g -O3 -Wall -DOCI_IMPORT_LINKAGE -DOCILIB_CHARSET_ANSI -DSIMULATION -I/usr/local/include -L/u01/app/oracle/product/11.1.0/db_1/lib -L/usr/local/lib -locilib linkedlist.c str_utils.c time_utils.c utils.c help.c funloader.c -o funloader
 *
 *
 * */



#include "defs.h"
#include "ocilib.h"
#include "str_utils.h"
#include "linkedlist.h"
#include "time_utils.h"
#include "help.h"
#include "ful_structs.h"
#include "ful_options.h"
#include <sys/sysinfo.h>
#include <assert.h>
#include <math.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>




/* -- function declerations -- */

void ErrorHandler( OCI_Error *err );
void InitUnloadVariables(void);
bool readUnloadQueryFile(void);
void setRecordInfo(OCI_Statement * stmt);
void setRowIdRanges( OCI_Statement * stmt );
rowid_info *getRowIdInfo( OCI_Statement * stmt, char *sql_stmt );
char *getRowIdSql( char * object_name );

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_ROWID_RANGES)
void printRowIdRanges( void );
void printRowIdInfo( rowid_info *rowidinfo );        
#endif

void parseConfigFile( const char *cfgfile ); 
void setopt(char *opt, char *optarg );
bool setPartitionList( void );
bool setSubPartitionList( void );
void printPartitionInfo( void );
void printSubPartitionInfo( void );

#ifdef FUL_ENABLE_AUTO_FETCH_SIZE
void setFetchSize( void );
#endif

void printOptions(void );
void printUnloadVariables( void );
void setCommandLineOptions( int argc, char **fakeargv );
void initOptions( void );
void plog(int log_type, char * logstr, bool  ptime , int log_level, ...);
void setlogfd( void );
bool setDBPartitionList(OCI_Statement * stmt);
bool setDBSubPartitionList(OCI_Statement * stmt);

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_SYS_INFO)
void printSysInfo( void );
#endif

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_DB_VERSION)
void printDBVersion( OCI_Connection *cn); 
#endif

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_UNLOAD_THREAD_DATA)
void printUnloadThreadData( unload_thread_data * );
#endif

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_LOAD_THREAD_DATA)
void printLoadThreadData( load_thread_data * ltd);
#endif

void unload( OCI_Thread *thread, void * node );
void load( OCI_Thread *thread_id, void * ltd );
void cleanup( unsigned short int );
void setPartitionLevel( OCI_Statement * stmt );
void generateCtlFile( void );
void setDelimiter(const char *src, char **dest, unsigned short int delimiter_type  );
void setXDelimiter(const char *src, char **dest, unsigned short delimiter_type ); 
int  getHexDigit(const char x);
char *getVisibleString(const char *strval);
void prepareUnloadQueryReWrite( void );
void UnloadQueryReWrite(   const char *rowid_from , const char *rowid_to
                         , const char *object_name, char **unload_query );

void setSqlldrCmd( unsigned short int thread_no, char **sqlldr_cmd);



/* -- global variables -- */


options o;                 /* options structure       */
unloadquery_metadata uqmd; /* unload query metadata   */
unload_variables uv;       /* unload variables        */
OCI_Connection* con;       /* connection descriptor   */

FILE *logfd ;  /* log file descriptor */

int main( int argc, char *argv[] )
{
 
   OCI_Statement* stmt;
   time_t start_time,end_time;
   time_t l_start_time,l_end_time;
   register unsigned short int i = 0;
   unsigned short int unload_thread_no = 0;
   char *config_file = NULL;
   char **fakeargv   = NULL;
   ListNode *tmpnode       = (ListNode *)NULL;
   ListNode *partl_node    = (ListNode *)NULL;
   ListNode *spartl_node   = (ListNode *)NULL;
   
   ListNode *sl_tmpnode    = (ListNode *)NULL;
   ListNode *wl_tmpnode    = (ListNode *)NULL;
   ListNode *su_tmpnode    = (ListNode *)NULL;
   ListNode *wu_tmpnode    = (ListNode *)NULL;
   
   LinkedList *utd_list    = (LinkedList *)NULL;
   LinkedList *ltd_list    = (LinkedList *)NULL;
   unload_thread_data *utd = (unload_thread_data *)NULL;
   load_thread_data   *ltd = (load_thread_data   *)NULL;
   
   char *truncate_stmt = NULL;

   fakeargv = (char **)safe_malloc( (argc + 1)*sizeof(char *) ); 

   time(&start_time); /* mark the start time of the execution */
   
  /* ........................... 
   * redirect the logs to stdout
   * until cmd options are set 
   * ........................... */
   setlogfd();
    
   /* initialize unload variables */
   InitUnloadVariables();
   
   initOptions( ); /* init options struct */

#ifdef FULL_DEBUG_AGGRESSIVE
   plog(FUL_EXEC_LOG,"Program Execution Started ...\n\n", TRUE,0);

   plog(FUL_EXEC_LOG,"Options initialized with default values\n\n", TRUE,0);
#endif

   /* argv faking */
   for(i = 0; i < argc; i++) 
   {
      fakeargv[i] = (char *)safe_malloc((strlen(argv[i]))*sizeof(char *));  
  		strncpy(fakeargv[i], argv[i], strlen(argv[i]) + 1);
      if(strcmp(fakeargv[i],"-CONFIG_FILE")==0 || strcmp(fakeargv[i] ,"--CONFIG_FILE")==0 ) 
         config_file = strdup(argv[i+1]);   
   }
   fakeargv[argc] = NULL;


   if(config_file) /* if config file given */
   {
#ifdef FULL_DEBUG_AGGRESSIVE
      plog(FUL_EXEC_LOG, "Using configuration file: %s\n\n"
                       , TRUE,0,config_file);
      
      plog(FUL_EXEC_LOG, "Parsing configuration file ...\n\n"  
                       , TRUE,0);
#endif      
      parseConfigFile( config_file ); /* parse the configuration file */  

#ifdef FULL_DEBUG_AGGRESSIVE
      plog( FUL_EXEC_LOG,"Configuration file is parsed.\n\n",TRUE,0);
#endif      

   }
#ifdef FULL_DEBUG_AGGRESSIVE   
   else
      plog(FUL_EXEC_LOG,"Using only command line options ...\n\n", TRUE,0);     
#endif      

   /* override with cmd line options */
   setCommandLineOptions(argc, fakeargv ); 

   /* options are clear now,we can reset the logfd*/
   setlogfd(); /* set log file descriptor if given */

#ifdef FULL_DEBUG_AGGRESSIVE
   plog( FUL_DEBUG_LOG,"Command line options are set\n\n",TRUE,0);
#endif      



   if(o.debugging) printOptions(); /* print raw option-value pairs */
   
   if(o.user_name == NULL) /* check connect user name */
   {
      plog(FUL_EXEC_LOG, "Usarname is not given! Quiting ...\n\n",TRUE,1);
      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }
   if(o.password == NULL) /* check conncet password */
   {
      plog(FUL_EXEC_LOG, "Pasword is not given! Quiting ...\n\n",TRUE,1);
      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }


   if(o.log_level > FUL_MAX_LOG_LEVEL )
   {
      plog(FUL_EXEC_LOG, "Loag level is greater than the maximum value %d !\n"
                         "Setting log level to %d ...\n\n" 
                       , TRUE,1
                       , FUL_MAX_LOG_LEVEL, FUL_MAX_LOG_LEVEL);

      o.log_level = FUL_MAX_LOG_LEVEL;   
   }
   
  
   /* ----------- Unload Query Handling ------------ */

   if(!o.unload_query && !o.unload_query_file)
   {
      plog(FUL_EXEC_LOG, "Unload query is not given !\n"
                         "Quitting ...\n\n",TRUE,1); 

      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }
   else if( o.unload_query_file )
   {
      if(o.unload_query)
      {
         plog(FUL_EXEC_LOG, "Both unload query and unload query file is given !\n"
                            "Favouring unload query file ...\n\n"
                          ,  TRUE,3 );
      }
      if(readUnloadQueryFile( ) == FALSE)
      { 
         plog(FUL_EXEC_LOG, "Trying to use unload query option ...\n\n"
                          , TRUE,1);

         if(o.unload_query == NULL)
         {
            plog(FUL_EXEC_LOG, "Unload query is not given !\n\n"
                               "Quitting ...\n\n", TRUE,1);
            
            cleanup(FUL_CLEAN_CODE_FAILURE);
            return EXIT_FAILURE;
         }
         else if(strlen(o.unload_query) < FUL_MIN_UQ_FILE_SIZE )
         {
            plog(FUL_EXEC_LOG , "Unload query size is less than minumum size %d !\n\n"
                              , TRUE,1
                              , FUL_MIN_UQ_FILE_SIZE);  

            cleanup(FUL_CLEAN_CODE_FAILURE);
            return EXIT_FAILURE;
         }
         else uqmd.unloadquery = o.unload_query;
      }
   }
   else
   {
      
      if(strlen(o.unload_query) < FUL_MIN_UQ_FILE_SIZE )
      {
         plog(FUL_EXEC_LOG, "Unload query size is less than minumum size %d !\n\n"
                       , TRUE,1
                       , FUL_MIN_UQ_FILE_SIZE);  

         cleanup(FUL_CLEAN_CODE_FAILURE);
         return EXIT_FAILURE;
      }
      
      uqmd.unloadquery = o.unload_query;
   }
   
   /* unload query is refined and set */
   plog(FUL_EXEC_LOG, "Unload Query is set\n\n",TRUE,3);


   
   /* ----------------- Set Parallel Degree -------------- */ 
   if( isNumber(o.par_degree) == FALSE || atoi(o.par_degree) < 0)
   {
      plog(FUL_EXEC_LOG, "Parallel degree \"%s\" is not a valid number !\n"
                          "Setting parallel degree to default %d ...\n\n"
                        , TRUE,2
                        , o.par_degree,FUL_DEFAULT_PARALLEL_DEGREE);

      uv.par_degree = FUL_DEFAULT_PARALLEL_DEGREE;
      
   }
   else if( atoi(o.par_degree) > FUL_MAX_PARALLEL_DEGREE )
   {

      plog(FUL_EXEC_LOG, "Parallel degree is greater than the maximum value %d !\n"
                         "Setting parallel degree to %d ...\n\n"
                       , TRUE,2
                       , FUL_MAX_PARALLEL_DEGREE,FUL_MAX_PARALLEL_DEGREE);

      uv.par_degree = FUL_MAX_PARALLEL_DEGREE;
    
   }
   else uv.par_degree = atoi(o.par_degree);  
 

   /* ---------- Set Connect Session Mode ---------- */  
   if(o.con_ses_mode == NULL)                       uv.con_ses_mode = OCI_SESSION_DEFAULT;
   else if(!strcasecmp(o.con_ses_mode,"DEFAULT") )  uv.con_ses_mode = OCI_SESSION_DEFAULT;  
   else if(!strcasecmp(o.con_ses_mode,"SYSDBA") )   uv.con_ses_mode = OCI_SESSION_SYSDBA; 
   else if(!strcasecmp(o.con_ses_mode,"SYSOPER") )  uv.con_ses_mode = OCI_SESSION_SYSOPER; 
   else
   {
      plog(FUL_EXEC_LOG, "%s is not a valid connect session mode!\n"
                         "Setting connect session mode to DEFAULT...\n\n"
                       , FALSE,1,o.con_ses_mode);
      
      o.con_ses_mode = OCI_SESSION_DEFAULT;
   }
   

   /* ---------- POST PROCESSING OPTIONS ---------- */  


   /* set parallelisation type */
   if( !strcasecmp(o.par_type,     "NONE" ) )        uv.par_type = FUL_PARALLEL_TYPE_NONE;
   else if( !strcasecmp(o.par_type,"PARTITION") )    uv.par_type = FUL_PARALLEL_TYPE_PARTITION;
   else if( !strcasecmp(o.par_type,"SUBPARTITION") ) uv.par_type = FUL_PARALLEL_TYPE_SUBPARTITION;
   else if( !strcasecmp(o.par_type,"ROWID") )        uv.par_type = FUL_PARALLEL_TYPE_ROWID;
   else
   {
      plog(FUL_EXEC_LOG, "Unknown parallel type %s !\n"
                         "Quiting...\n\n"
                       , TRUE,1, o.par_type);

      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;   
   } /* parallel type is set */


   uqmd.mt = (main_table *)safe_malloc(sizeof(main_table));
   uqmd.mt->allias = NULL;
   uqmd.mt->part_list = NULL;
   uqmd.mt->subpart_list = NULL;


   if( strlen(o.mtab_name) > FUL_MAX_TABLE_NAME_LEN )
   {
      plog(FUL_EXEC_LOG, "Table name length is greater than maximum table name length %d !\n"
                         "Quiting ...\n\n"
                       , TRUE,1
                       , FUL_MAX_TABLE_NAME_LEN);  

      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }

   if( strlen(o.mtab_owner) > FUL_MAX_USER_NAME_LEN )
   {
      plog(FUL_EXEC_LOG, "Table schema name length is greater than maximum schema name length %d !\n"
                         "Quiting ...\n\n"
                       , TRUE,1
                       , FUL_MAX_TABLE_NAME_LEN);  

      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }
      

   asprintf(&(uqmd.mt->name),  o.mtab_name); /* main table name  */
   asprintf(&(uqmd.mt->owner), o.mtab_owner);/* main table owner */


   /* ----------- Partition Level Handling ----------- */
   if( !strcasecmp(o.mtab_part_level,"NONE" )  ) 
      uqmd.mt->part_level = FUL_PARTITION_LEVEL_NONE;
   else if( !strcasecmp(o.mtab_part_level,"PARTITION") ) 
      uqmd.mt->part_level = FUL_PARTITION_LEVEL_PARTITION;
   else if( !strcasecmp(o.mtab_part_level,"SUBPARTITION") )
      uqmd.mt->part_level = FUL_PARTITION_LEVEL_SUBPARTITION;

   
   /* ---------- Create Connection ---------- */  
   if(! OCI_Initialize(ErrorHandler, NULL, OCI_ENV_DEFAULT | OCI_ENV_THREADED ) )
		return EXIT_FAILURE;
 
   plog(FUL_EXEC_LOG,"Connecting to %s...\n\n",TRUE,1, o.net_service_name);
      
   con  = OCI_ConnectionCreate(  o.net_service_name, o.user_name
                               , o.password, uv.con_ses_mode);
   if( con == NULL ) 
   {
      plog(FUL_EXEC_LOG, "Unable to connect %s with username %s password %s !\n"
                       , TRUE,1
                       , o.net_service_name, o.user_name, o.password);

      return EXIT_FAILURE;
   }
   
   plog(FUL_EXEC_LOG,"Connection established.\n\n",TRUE,1);
  
#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_DB_VERSION)
   printDBVersion( con );
#endif

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_SYS_INFO)   
   printSysInfo();
#endif

   stmt = OCI_StatementCreate(con);

   if( !strcasecmp(o.mtab_part_level, "UNKNOWN") )
   {   
      plog( FUL_EXEC_LOG, "Table paritition level UNKNOWN. Finding partition level ...\n\n", TRUE,2 );

      setPartitionLevel( stmt ); /* find main table partition level */
      assert(uqmd.mt->part_level != FUL_PARTITION_LEVEL_UNKNOWN);
      
      plog( FUL_EXEC_LOG, "Table partition level : %s\n\n"
                        , TRUE,2
                        , uqmd.mt->part_level==FUL_PARTITION_LEVEL_PARTITION ? "PARTITION":
                          uqmd.mt->part_level==FUL_PARTITION_LEVEL_SUBPARTITION ? "SUBPARTITION":"NONE");

      if(uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE) uv.set_partlist = FALSE;
      if(uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION ) uv.set_subpartlist = FALSE;
   }

   /* Check Consistency for Partition Options */  
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE && (uv.set_partlist || o.partlist) )
   {
      plog(FUL_EXEC_LOG , "Inconsistency detected!\n"
                          "Table partition level is set to NONE and $PARTITION_NAME option is given.\n"
                          "Quiting ...\n\n"
                        , TRUE,1);
      
      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE && (uv.set_subpartlist || o.subpartlist) )
   {
      plog(FUL_EXEC_LOG , "Inconsistency detected!\n"
                          "Table partition level is set to NONE and $SUBPARTITION_NAME option is given.\n"
                          "Quiting ...\n\n"
                        , TRUE,1);
   
      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION && (uv.set_subpartlist ||o.subpartlist != NULL)  )
   {
      plog(FUL_EXEC_LOG , "Inconsistency detected!\n"
                          "Table partition level is set to PARTITION and $SUBPARTITION_NAME option is given.\n"
                          "Quiting ...\n\n"
                        , TRUE,1);
   
      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }

   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION && (uv.set_partlist ||o.partlist != NULL) )
   {
      plog(FUL_EXEC_LOG , "Inconsistency detected!\n"
                          "Table partition level is set to SUBPARTITION and $PARTITION_NAME option is given.\n"
                          "Quiting ...\n\n"
                        , TRUE,1);
   
      cleanup(FUL_CLEAN_CODE_FAILURE);
      return EXIT_FAILURE;
   }

   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION && 
      o.partlist == NULL && uv.set_partlist == FALSE )
   {
      plog(FUL_EXEC_LOG , "Inconsistency detected!\n"
                          "Table partition level is set to PARTITION and $PARTITION_NAME option is NOT given!\n"
                          "Quiting ...\n\n"
                        , TRUE,1);
   
      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }

   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION && 
      o.subpartlist == NULL && uv.set_subpartlist == FALSE )
   {
      plog(FUL_EXEC_LOG , "Inconsistency detected!\n"
                          "Table partition level is set to SUBPARTITION and $SUBPARTITION_NAME option is NOT given!\n"
                          "Quiting ...\n\n"
                        , TRUE,1);
   
      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }

   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION ) 
   {
      if( uv.set_partlist )
      {
         plog( FUL_EXEC_LOG , "Finding and setting partition names ...\n\n"
                            , TRUE,2);
         if(FALSE == setDBPartitionList( stmt )) /* set partition names */  
         { 
            plog(FUL_EXEC_LOG, "Unable to retrieve partition names from database!\n"
                               "Quiting ...\n\n",TRUE,1);

            cleanup(FUL_CLEAN_CODE_FAILURE);
         }
      }
      else
      {
         plog( FUL_EXEC_LOG , "Setting partition names ...\n\n"
                            , TRUE,2);
         setPartitionList( ); /* set partition names */  
      }
   }

   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION  ) 
   {
      if( uv.set_subpartlist )
      {
         plog( FUL_EXEC_LOG , "Finding and setting subpartition names ...\n\n"
                            , TRUE,2);
            
         /* retrieve and set subpartition name list */  
         if(FALSE == setDBSubPartitionList( stmt ) )
         {
            plog(FUL_EXEC_LOG, "Unable to retrieve subpartition names from database!\n"
                               "Quiting ...\n\n",TRUE,1);

            cleanup(FUL_CLEAN_CODE_FAILURE);
         } 
      }
      else
      {
         plog( FUL_EXEC_LOG , "Setting subpartition names ...\n\n"
                            , TRUE,2);
            
         /* retrieve and set partition name list */  
         setSubPartitionList( ); 
      }
   }
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE &&
      uv.par_type == FUL_PARALLEL_TYPE_PARTITION )
   {
      plog(FUL_EXEC_LOG, "Inconsistency detected!\n"
                         "Table partition level is NONE and parallel type is set to PARTITION\n"
                         "Qiiting ...\n"
                       , TRUE,1 );

      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   } 
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE &&
          uv.par_type == FUL_PARALLEL_TYPE_PARTITION )
   {
      plog(FUL_EXEC_LOG,  "Inconsistency detected!\n"
                          "Table partition level is NONE and parallel type is set to PARTITION\n"
                          "Qiiting ...\n"
                       ,  TRUE,1 );

      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE &&
          uv.par_type == FUL_PARALLEL_TYPE_SUBPARTITION )
   {
      plog(FUL_EXEC_LOG, "Inconsistency detected!\n"
                         "Table partition level is NONE and parallel type is set to SUBPARTITION\n"
                         "Qiiting ...\n"
                       , TRUE,1 );

      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION &&
         uv.par_type == FUL_PARALLEL_TYPE_SUBPARTITION )
   {
      plog(FUL_EXEC_LOG, "Inconsistency detected!\n"
                         "Table partition level is PARTITION and parallel type is set to SUBPARTITION\n"
                         "Qiiting ...\n"
                       , TRUE,1 );

      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION &&
         uv.par_type == FUL_PARALLEL_TYPE_PARTITION )
   {
      plog(FUL_EXEC_LOG, "Inconsistency detected!\n"
                         "Table partition level is SUBPARTITION and parallel type is set to PARTITION\n"
                         "Qiiting ...\n"
                       , TRUE,1 );

      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }

   /* ----------- Rowid Range Handling ----------- */
   if( uv.par_type == FUL_PARALLEL_TYPE_ROWID )
   {
      plog(FUL_EXEC_LOG,"Setting rowid ranges ...\n\n",TRUE,2);

      setRowIdRanges( stmt );
      
      plog(FUL_EXEC_LOG,"Rowid ranges are set. \n\n",TRUE,2);

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_ROWID_RANGES)
      plog(FUL_DEBUG_LOG,"Printing rowid ranges ...\n\n",TRUE,0);      
      printRowIdRanges();
#endif

   }
      
   if( !o.ctl_file_name && (o.gen_ctl_file || o.load_data) ) /* record info settings */
   {
      plog(FUL_EXEC_LOG, "Setting unload query record information ...\n\n",TRUE,2);
      
      setRecordInfo( stmt );
   
      plog(FUL_EXEC_LOG, "Unload query record information is set. \n\n",TRUE,2);   
   }
   
   /* ----------- Delimiter Handling ------- */

   if( o.xrecord_delimiter )
   {
      if(o.record_delimiter)
      {
         plog(FUL_EXEC_LOG , "Both \"record delimiter\" and \"hex record delimiter\""
                             "is given.\n Favouring \"hex record delimiter\".\n\n"
                           , TRUE,2); 
      }

      setXDelimiter(o.xrecord_delimiter, &(uv.record_delimiter), FUL_RECORD_DELIMITER);

   } 
   else if(o.record_delimiter)  
   {
      setDelimiter(o.record_delimiter, &(uv.record_delimiter), FUL_RECORD_DELIMITER);
   }
   else if( !o.record_delimiter && !o.xrecord_delimiter)
   {
      plog(FUL_EXEC_LOG, "Record delimiter is NOT given\n"
                         "Using newline character ...\n\n" , TRUE,4);
      
      uv.record_delimiter = (char *)safe_malloc(2*sizeof(char));
      *(uv.record_delimiter)   = '\n';
      *(uv.record_delimiter+1) = '\0';   
   }
   
   if( o.xfield_delimiter )
   {
      if(o.field_delimiter)
      {
         plog(FUL_EXEC_LOG , "Both \"field delimiter\" and \"hex field delimiter\""
                             "is given.\n Favouring \"hex field delimiter\".\n\n"
                           , TRUE,2); 
      }

      setXDelimiter(o.xfield_delimiter, &(uv.field_delimiter), FUL_FIELD_DELIMITER );
   } 
   else if(o.field_delimiter)  
   {
      setDelimiter(o.field_delimiter, &(uv.field_delimiter), FUL_FIELD_DELIMITER);
   }
   else if( !o.field_delimiter && !o.xfield_delimiter)
   {
      plog(FUL_EXEC_LOG, "Field delimiter is NOT given\n"
                         "Using empty string ...\n\n" , TRUE,4);

      uv.field_delimiter = (char *)safe_malloc(2*sizeof(char));
      strcpy(uv.field_delimiter, "");
      *(uv.field_delimiter+1) = '\0';
   }

   /* ----------- Setting Fetch Size  ----------- */
#ifdef FUL_ENABLE_AUTO_FETCH_SIZE
   if( !strcasecmp(o.fetch_size,"AUTO") )
   {
      plog(FUL_EXEC_LOG, "FETCH_SIZE option is set to AUTO. Setting prefetch size ...\n\n"
                       , TRUE,3);   

      setFetchSize();

      plog(FUL_EXEC_LOG, "Fetch size is set to %d.\n\n",TRUE,3,uv.fetch_size);
   }
#endif

   if(isNumber(o.fetch_size) == FALSE)
   {
      plog(FUL_EXEC_LOG, "Invalid prefetch size \"%s\" is given!"
                     "Setting prefetch size to default %d ...\n\n"
                   , TRUE,2
                   , o.fetch_size,FUL_DEFAULT_FETCH_SIZE );
      
      uv.fetch_size = FUL_DEFAULT_FETCH_SIZE;
   }
   else
      uv.fetch_size = FUL_DEFAULT_FETCH_SIZE;
   

   /* -- set date format -- */
   if( OCI_SetDefaultFormatDate ( con, o.date_format ) == FALSE )	  
   {
      plog(FUL_EXEC_LOG, "Invalid date format %s\n"
                         "Quiting ...\n\n"
                       , TRUE,1 );
        
      cleanup( FUL_CLEAN_CODE_FAILURE );
      return EXIT_FAILURE;
   }

      /* Generate CTL File */
   if( o.gen_ctl_file == TRUE )
   {
      plog(FUL_EXEC_LOG, "Generating control file for SQL-Loader...\n\n",TRUE,3);
      generateCtlFile();
   }


   time(&end_time);
   plog(FUL_DEBUG_LOG,  "%lf seconds elapsed before unload start\n\n"
                     ,  TRUE,0
                     ,  difftime(end_time, start_time));


   if(o.load_data)
   {   
      /*sqlldr username@server/password control=loader.ctl*/
      
      /* load thread data list */
      ltd_list = (LinkedList *)ListCreate();
      
      tmpnode = (ListNode *)utd_list->head;
   }

   if(o.unload2pipe)
   {
      uv.unloadstream_type=FUL_UNLOAD_STREAM_TYPE_PIPE;
   }   

   plog(FUL_DEBUG_LOG, "Constucting unload data ...\n\n"
                     , TRUE,0);

   
   utd_list = (LinkedList *)ListCreate();
   
   /* XXX FUL_PARALLEL_TYPE_NONE  XXX */
   if( uv.par_type == FUL_PARALLEL_TYPE_NONE )
   {
      utd = (unload_thread_data *)safe_malloc(sizeof(unload_thread_data));
      
      utd->unload_query = strdup(uqmd.unloadquery);

      utd->ulog      = (ul_logs *)safe_malloc(sizeof(ul_logs));
      utd->ulog->row_count = 0;
      utd->thread_no = 1;
      utd->thread_id = OCI_ThreadCreate();
      asprintf(&(utd->unloadfile),"%s_%03d",o.unload_file,1);

      tmpnode = (ListNode *)ListAppend( utd_list, sizeof(unload_thread_data)); 
      tmpnode->data = (ListNode *)utd;

      if(o.load_data)
      {
         ltd = (load_thread_data *)safe_malloc(sizeof(load_thread_data));         
         ltd->llog      = (ul_logs *)safe_malloc(sizeof(ul_logs));      
         ltd->thread_no = 1;
         ltd->thread_id = OCI_ThreadCreate();
         asprintf(&(ltd->loadfile),"%s_%03d",o.unload_file,1);
        
         setSqlldrCmd( 1, &(ltd->sqlldr_cmd));

         tmpnode = (ListNode *)ListAppend( ltd_list, sizeof(load_thread_data)); 
         tmpnode->data = (ListNode *)ltd;
      }


   } /* XXX FUL_PARALLEL_TYPE_ROWID  XXX */ 
   else if( uv.par_type == FUL_PARALLEL_TYPE_ROWID )
   {
      unload_thread_no = 0;
      
      if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE ) 
      {
         for( i = 0; i<uqmd.mt->rowidinfo->rowid_range_count; i++)
         {
            utd = (unload_thread_data *)safe_malloc(sizeof(unload_thread_data));
            
            
            prepareUnloadQueryReWrite();
            UnloadQueryReWrite(  uqmd.mt->rowidinfo->rowid_from[i]
                               , uqmd.mt->rowidinfo->rowid_to[i]
                               , NULL 
                               , &(utd->unload_query) );
            
            utd->ulog      = (ul_logs *)safe_malloc(sizeof(ul_logs));
            utd->ulog->row_count = 0;
            utd->thread_no = unload_thread_no;
            utd->thread_id = OCI_ThreadCreate();
            asprintf(&(utd->unloadfile),"%s_%03d",o.unload_file,i);

            tmpnode = (ListNode *)ListAppend( utd_list, sizeof(unload_thread_data *));
            tmpnode->data = (ListNode *)utd;

            if(o.load_data)
            {
               ltd = (load_thread_data *)safe_malloc(sizeof(load_thread_data));         
               ltd->llog      = (ul_logs *)safe_malloc(sizeof(ul_logs));      
               ltd->thread_no = unload_thread_no;
               ltd->thread_id = OCI_ThreadCreate();
               asprintf(&(ltd->loadfile),"%s_%03d",o.unload_file,unload_thread_no);
               
               setSqlldrCmd( unload_thread_no , &(ltd->sqlldr_cmd));

               tmpnode = (ListNode *)ListAppend( ltd_list, sizeof(load_thread_data)); 
               tmpnode->data = (ListNode *)ltd;
            }

            unload_thread_no++;

         } /* end for */
      } /* end FUL_PARTITION_LEVEL_NONE */
      else if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION)
      {
         partl_node = (ListNode *)(uqmd.mt->part_list->head);
         tmpnode = NULL;
         unload_thread_no = 0;   
         
         while(partl_node != NULL)
         {
            for(i = 0; i< (int)( ((partition *)(partl_node->data))->rowidinfo->rowid_range_count); i++)
            {
               utd = (unload_thread_data *)safe_malloc(sizeof(unload_thread_data));
                       
               prepareUnloadQueryReWrite();
               UnloadQueryReWrite( ((partition *)(partl_node->data))->rowidinfo->rowid_from[i]
                                  , ((partition *)(partl_node->data))->rowidinfo->rowid_to[i]
                                  , NULL
                                  , &(utd->unload_query) );
               
               utd->ulog      = (ul_logs *)safe_malloc(sizeof(ul_logs));
               utd->ulog->row_count = 0;
               utd->thread_no = unload_thread_no;      
               asprintf(&(utd->unloadfile),"%s_%03d",o.unload_file,unload_thread_no);

               if( (utd->thread_id = OCI_ThreadCreate())==NULL)
               {
                  plog(FUL_EXEC_LOG, "Failed to create thread handle!\n"
                                     "Quiting ..."
                                   , TRUE,1  ) ;   
                  
                  cleanup( FUL_CLEAN_CODE_FAILURE );
                  return EXIT_FAILURE;
               }

               tmpnode = (ListNode *)ListAppend( utd_list, sizeof(unload_thread_data *));
               tmpnode->data = (ListNode *)utd;

               if(o.load_data)
               {
                  ltd = (load_thread_data *)safe_malloc(sizeof(load_thread_data));         
                  ltd->llog      = (ul_logs *)safe_malloc(sizeof(ul_logs));      
                  ltd->thread_no = unload_thread_no;
                  ltd->thread_id = OCI_ThreadCreate();
                  asprintf(&(ltd->loadfile),"%s_%03d",o.unload_file,unload_thread_no);
                  
                  setSqlldrCmd( unload_thread_no , &(ltd->sqlldr_cmd));

                  tmpnode = (ListNode *)ListAppend( ltd_list, sizeof(load_thread_data)); 
                  tmpnode->data = (ListNode *)ltd;
               }

               unload_thread_no++;

            } /* end for */

            partl_node = partl_node->next;
         } /* end while */

      }
      else if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION )
      {
         spartl_node = (ListNode *)(uqmd.mt->subpart_list->head);
         tmpnode = NULL;
         unload_thread_no = 0; 

         while(spartl_node != NULL)
         {
            for(i = 0; i< (int)( ((subpartition *)(spartl_node->data))->rowidinfo->rowid_range_count); i++)
            {
               utd = (unload_thread_data *)safe_malloc(sizeof(unload_thread_data));
               
               prepareUnloadQueryReWrite();
               UnloadQueryReWrite(  ((subpartition *)(spartl_node->data))->rowidinfo->rowid_from[i]
                                  , ((subpartition *)(spartl_node->data))->rowidinfo->rowid_to[i]
                                  , NULL
                                  , &(utd->unload_query) );

               
               utd->ulog      = (ul_logs *)safe_malloc(sizeof(ul_logs));
               utd->ulog->row_count = 0;
               utd->thread_no = unload_thread_no;     
               asprintf(&(utd->unloadfile),"%s_%03d",o.unload_file,unload_thread_no); 
               
               utd->thread_id = OCI_ThreadCreate();   

               tmpnode = (ListNode *)ListAppend( utd_list, sizeof(unload_thread_data *));
               tmpnode->data = (ListNode *)utd;
      
               if(o.load_data)
               {
                  ltd = (load_thread_data *)safe_malloc(sizeof(load_thread_data));         
                  ltd->llog      = (ul_logs *)safe_malloc(sizeof(ul_logs));      
                  ltd->thread_no = unload_thread_no;
                  ltd->thread_id = OCI_ThreadCreate();
                  asprintf(&(ltd->loadfile),"%s_%03d",o.unload_file,unload_thread_no);
                  
                  setSqlldrCmd( unload_thread_no , &(ltd->sqlldr_cmd));

                  tmpnode = (ListNode *)ListAppend( ltd_list, sizeof(load_thread_data)); 
                  tmpnode->data = (ListNode *)ltd;
               }
               
               unload_thread_no++;

            } /* end for */

            spartl_node = spartl_node->next;
         } /* end while */

      } 
      else
      {
         plog(FUL_EXEC_LOG, "Unexpected error occured during unload data initialization!\n"
                            "Quiting ...\n\n"
                          , TRUE,1);
         cleanup( FUL_CLEAN_CODE_FAILURE );
      
         return EXIT_FAILURE;
      }

   } /* XXX FUL_PARALLEL_TYPE_PARTITION FUL_PARALLEL_TYPE_SUBPARTITION XXX */
   else if( uv.par_type == FUL_PARALLEL_TYPE_PARTITION )
   {

      partl_node = (ListNode *)(uqmd.mt->part_list->head);               
      tmpnode = NULL;
      unload_thread_no = 0;

      while(partl_node != NULL)
      {
         utd = (unload_thread_data *)safe_malloc(sizeof(unload_thread_data));
         
         prepareUnloadQueryReWrite();
         UnloadQueryReWrite( NULL, NULL
                                 , (char *)(((partition *)(partl_node->data))->name)
                                 , &(utd->unload_query) );
            
         utd->ulog      = (ul_logs *)safe_malloc(sizeof(ul_logs));
         utd->thread_no = unload_thread_no;      
         asprintf(&(utd->unloadfile),"%s_%03d",o.unload_file,unload_thread_no);
         
         utd->thread_id = OCI_ThreadCreate();   
         
         tmpnode = (ListNode *)ListAppend( (LinkedList *)utd_list, sizeof(unload_thread_data *));
         tmpnode->data = (unload_thread_data *)utd;
         
         partl_node = partl_node->next;

         if(o.load_data)
         {
            ltd = (load_thread_data *)safe_malloc(sizeof(load_thread_data));         
            ltd->llog      = (ul_logs *)safe_malloc(sizeof(ul_logs));      
            ltd->thread_no = unload_thread_no;
            ltd->thread_id = OCI_ThreadCreate();
            asprintf(&(ltd->loadfile),"%s_%03d",o.unload_file,unload_thread_no);
            
            setSqlldrCmd( unload_thread_no , &(ltd->sqlldr_cmd));

            tmpnode = (ListNode *)ListAppend( ltd_list, sizeof(load_thread_data)); 
            tmpnode->data = (ListNode *)ltd;
         }

         unload_thread_no++;
         
      } /* end while */
   }
   else if(uv.par_type == FUL_PARALLEL_TYPE_SUBPARTITION)
   {
      spartl_node = (ListNode *)(uqmd.mt->subpart_list->head);
      tmpnode = NULL;
      unload_thread_no = 0;

      while(spartl_node != NULL)
      {
         utd = (unload_thread_data *)safe_malloc(sizeof(unload_thread_data));
         
         prepareUnloadQueryReWrite();
         UnloadQueryReWrite( NULL, NULL
                                 ,  (char *)(((subpartition *)(spartl_node->data))->name)
                                 ,  &(utd->unload_query) );
         
         utd->ulog      = (ul_logs *)safe_malloc(sizeof(ul_logs));
         utd->thread_no = unload_thread_no;      
         asprintf(&(utd->unloadfile),"%s_%03d",o.unload_file,unload_thread_no);

         utd->thread_id = OCI_ThreadCreate();   
         
         tmpnode = (ListNode *)ListAppend( (LinkedList *)utd_list, sizeof(unload_thread_data *));
         tmpnode->data = (unload_thread_data *)utd;
         
         spartl_node = spartl_node->next;

         if(o.load_data)
         {
            ltd = (load_thread_data *)safe_malloc(sizeof(load_thread_data));         
            ltd->llog      = (ul_logs *)safe_malloc(sizeof(ul_logs));      
            ltd->thread_no = unload_thread_no;
            ltd->thread_id = OCI_ThreadCreate();
            asprintf(&(ltd->loadfile),"%s_%03d",o.unload_file,unload_thread_no);
            
            setSqlldrCmd( unload_thread_no , &(ltd->sqlldr_cmd));

            tmpnode = (ListNode *)ListAppend( ltd_list, sizeof(load_thread_data)); 
            tmpnode->data = (ListNode *)ltd;
         }
         
         unload_thread_no++;
         
      } /* end while */

   }
   else
   {
      plog(FUL_EXEC_LOG, "Unexpected error occured during unload data initialization!\n"
                         "Quiting ...\n\n"
                       , TRUE,1);

      cleanup( FUL_CLEAN_CODE_FAILURE );
      
      return EXIT_FAILURE;
   }


   /* -- truncate target table -- */
   if(o.truncate_target_table == TRUE)
   {
      plog(FUL_EXEC_LOG,"Truncating target table ...\n\n",TRUE,3);
      
      asprintf(&truncate_stmt, "TRUNCATE TABLE %s.%s"
                            , o.target_table_owner
                            , o.target_table_name);

      if( TRUE == OCI_ExecuteStmt(stmt, truncate_stmt) )
         plog(FUL_EXEC_LOG, "Target table truncated.\n\n",TRUE,3);
      else
      {
         plog(FUL_EXEC_LOG, "Failed to truncate target table %s.%s "
                            "Quiting ...\n"           
                          , TRUE,1
                          , o.target_table_owner
                          , o.target_table_name);

         cleanup(FUL_CLEAN_CODE_FAILURE);
      }
   }
   
   
   plog(FUL_EXEC_LOG , "Starting unload in %s mode...\n\n"
                     , TRUE,2
                     , uv.par_degree > 1 ? "PARALLEL":"NONPARALLEL" );   

   
   /* set load start time */
   asprintf(&(uv.ulog.start_time), get_local_time());
   su_tmpnode = wu_tmpnode = (ListNode *)utd_list->head;
   time(&start_time);
   
   if(o.load_data)
   {  
      if(uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_PIPE)
      {
         asprintf(&(uv.llog.start_time), get_local_time());
         time(&l_start_time);
      }
      sl_tmpnode = wl_tmpnode = (ListNode *)ltd_list->head; 
   }
   
   /* ------------------- [UNLOAD] ------------------------------- */   
   for(i=1; (su_tmpnode != NULL); i++)
   {    
      
      /* -------------- [Pipe] ----------------- */
      if(uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_PIPE && o.load_data )
      {
#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_LOAD_THREAD_DATA)
         printLoadThreadData((load_thread_data *)(sl_tmpnode->data));
#endif
         plog(FUL_DEBUG_LOG, "Load session %d started.\n\n"
                           , TRUE,0
                           , ((load_thread_data *)(sl_tmpnode->data))->thread_no);
         
         /* create a named pipe with read/write permissions to the user */    
         mkfifo(((load_thread_data *)(sl_tmpnode->data))->loadfile, S_IRUSR | S_IWUSR );

         OCI_ThreadRun(  ((load_thread_data *)(sl_tmpnode->data))->thread_id
                       , load
                       , (void *)(sl_tmpnode->data) );
         
         sl_tmpnode = sl_tmpnode->next;
      
      }
      /* -------------- [/Pipe] ----------------- */


      plog(FUL_DEBUG_LOG, "Unload session %d started.\n\n"
                        , TRUE,0
                        , ((unload_thread_data *)(su_tmpnode->data))->thread_no);
      
#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_UNLOAD_THREAD_DATA)
      printUnloadThreadData((unload_thread_data *)(su_tmpnode->data));      
#endif
      
      OCI_ThreadRun(  ((unload_thread_data *)(su_tmpnode->data))->thread_id
                    , unload
                    , (void *)(su_tmpnode->data) );
      
      su_tmpnode = su_tmpnode->next;

      if(i == uv.par_degree || !su_tmpnode)
      {
         i = 0;
         while( wu_tmpnode && wu_tmpnode != su_tmpnode)
         {
            
            
            OCI_ThreadJoin(((unload_thread_data *)(wu_tmpnode->data))->thread_id);
            OCI_ThreadFree(((unload_thread_data *)(wu_tmpnode->data))->thread_id);
            
            uv.ulog.row_count += ((unload_thread_data *)(wu_tmpnode->data))->ulog->row_count;
            uv.ulog.serial_elapsed_time += ((unload_thread_data *)(wu_tmpnode->data))->ulog->elapsed_time;
               
            plog(FUL_DEBUG_LOG, "Unload session %d finished.\n\n"
                              , TRUE,0
                              , ((unload_thread_data *)(wu_tmpnode->data))->thread_no);

            wu_tmpnode = wu_tmpnode->next;
            
            if(o.load_data && uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_PIPE)
            {
               OCI_ThreadJoin(((load_thread_data *)(wl_tmpnode->data))->thread_id);
               OCI_ThreadFree(((load_thread_data *)(wl_tmpnode->data))->thread_id);         
         
               uv.llog.serial_elapsed_time += ((load_thread_data *)(wl_tmpnode->data))->llog->elapsed_time;
               plog(FUL_DEBUG_LOG, "Load session %d finished.\n\n"
                                 , TRUE,0
                                 , ((load_thread_data *)(wl_tmpnode->data))->thread_no);        
      
               wl_tmpnode = wl_tmpnode->next;
            }
               
         } /* end while */

      } /* end i == uv.par_degree */
   } /* end for */
   
   time(&end_time);
   uv.ulog.elapsed_time = difftime(end_time, start_time);
   asprintf(&(uv.ulog.end_time), get_local_time());

   if(o.load_data && uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_PIPE)
   {    
      time(&l_end_time);
      uv.llog.elapsed_time = difftime(l_end_time, l_start_time);
      asprintf(&(uv.llog.end_time), get_local_time());
   }
   /* --------------------- [/UNLOAD] ----------------------------- */

   
   /* if file start loading */
   if( o.load_data && uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_FILE )
   {

      asprintf(&(uv.llog.start_time), get_local_time());
      time(&l_start_time);

      for(i=1; (sl_tmpnode != NULL); i++)
      {   
         plog(FUL_DEBUG_LOG, "Load session %d started.\n\n"
                           , TRUE,0
                           , ((load_thread_data *)(sl_tmpnode->data))->thread_no);

         
#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_LOAD_THREAD_DATA)
         printLoadThreadData((load_thread_data *)(wl_tmpnode->data));         
#endif

         OCI_ThreadRun(  ((load_thread_data *)(sl_tmpnode->data))->thread_id
                       , load
                       , (void *)(sl_tmpnode->data) );
         
         sl_tmpnode = sl_tmpnode->next;

         if(i == uv.par_degree )
         {
            i = 0;
            while(wl_tmpnode && wl_tmpnode != sl_tmpnode)
            {
               
               OCI_ThreadJoin(((load_thread_data *)(wl_tmpnode->data))->thread_id);
               OCI_ThreadFree(((load_thread_data *)(wl_tmpnode->data))->thread_id);
            
               uv.llog.serial_elapsed_time += ((load_thread_data *)(wl_tmpnode->data))->llog->elapsed_time;
               
               plog(FUL_DEBUG_LOG, "Load session %d finished.\n\n"
                                 , TRUE,0
                                 , ((load_thread_data *)(wl_tmpnode->data))->thread_no);

               wl_tmpnode = wl_tmpnode->next;

            } /* end while */
         } /* end if */
      } /* end for */
   }/* end if */
    

/* -------------------------------------------------------------------------------------------------- */


   /* ---------- EXECUTION LOGS ---------- */ 
   plog( FUL_EXEC_LOG,
            "\n"
            "+ ---------------------------------------------------------- +\n"
            "+  EXECUTION RESULTS                                         +\n"
            "+ ---------------------------------------------------------- +\n\n"
            ,FALSE,1);

		
   plog(FUL_EXEC_LOG, " --- Unload Result Logs --- \n"
                      "------------------------------ \n"
                        
                      "Unload stream type ..........: %s\n"
                      "Unload row count ............: %d\n"
                      "Elapsed time ................: %ld (sec)\n"
                      "Unload start time ...........: %s\n"
                      "Unload end time .............: %s\n"
                      "Serial elapsed time .........: %ld (sec)\n"
                      "------------------------------ \n\n"
                    , FALSE,1
                    , uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_FILE 
                        ? "File":"Pipe"
                    , uv.ulog.row_count, uv.ulog.elapsed_time
                    , uv.ulog.start_time,uv.ulog.end_time
                    , uv.ulog.serial_elapsed_time );

   if(o.load_data)
   {
      plog(FUL_EXEC_LOG, " --- Load Result Logs ---\n"
                      "------------------------------\n"
                        
                      "Load stream type ............: %s\n"
                      "Elapsed time ................: %ld (sec)\n"
                      "Load start time .............: %s\n"
                      "Load end time ...............: %s\n"
                      "Serial elapsed time .........: %ld (sec)\n"
                      "------------------------------ \n\n"
                    , FALSE,1
                    , uv.unloadstream_type==FUL_UNLOAD_STREAM_TYPE_FILE 
                        ? "File":"Pipe"
                    , uv.llog.elapsed_time
                    , uv.llog.start_time,uv.llog.end_time
                    , uv.llog.serial_elapsed_time );
   }   
  
   if( o.log_level >= 4 )
   {  /* ----------------------------------------
       * for relatively time consuming operations
       * log level can be checked in here 
       * --------------------------------------- */
      tmpnode = (ListNode *)utd_list->head;

      plog( FUL_EXEC_LOG, "  --- Unload Log Details --- \n"
                        , FALSE,1);

      while( tmpnode )
      {
         
         plog(FUL_EXEC_LOG, "-------------------------------\n"
                            "Unload session ...............: %d\n"
                            "Unload row count .............: %ld\n"
                            "Unload start time ............: %s\n"
                            "Unload end time ..............: %s\n"
                            "Elapsed time .................: %ld (sec)\n"    
                          , FALSE,4
                          , ((unload_thread_data *)(tmpnode->data))->thread_no
                          , ((unload_thread_data *)(tmpnode->data))->ulog->row_count
                          , ((unload_thread_data *)(tmpnode->data))->ulog->start_time
                          , ((unload_thread_data *)(tmpnode->data))->ulog->end_time
                          , ((unload_thread_data *)(tmpnode->data))->ulog->elapsed_time );
         
         plog(FUL_EXEC_LOG, "Unload file ..................: %s\n"
                            "Unload query .................: %s\n"
                            "-------------------------------\n"  
                          , FALSE,5
                          , ((unload_thread_data *)(tmpnode->data))->unloadfile 
                          , ((unload_thread_data *)(tmpnode->data))->unload_query);

         plog(FUL_EXEC_LOG,"\n",FALSE,4);
        
         tmpnode = tmpnode->next;
      } /* end while */

      if(o.load_data)
      {
         
         tmpnode = (ListNode *)ltd_list->head;

         plog( FUL_EXEC_LOG, "\n\n"
                             "  --- Unload Log Details --- \n"
                           , FALSE,1);   

         while( tmpnode )
         {
         
            plog(FUL_EXEC_LOG, "-------------------------------\n"
                               "Load session .................: %d\n"
                               "Load start time ..............: %s\n"
                               "Load end time ................: %s\n"
                               "Elapsed time .................: %ld (sec)\n"    
                             , FALSE,4
                             , ((load_thread_data *)(tmpnode->data))->thread_no
                             , ((load_thread_data *)(tmpnode->data))->llog->start_time
                             , ((load_thread_data *)(tmpnode->data))->llog->end_time
                             , ((load_thread_data *)(tmpnode->data))->llog->elapsed_time );
         
            plog(FUL_EXEC_LOG, "Load file ....................: %s\n"
                               "Load query ...................: %s\n"
                               "-------------------------------\n"  
                             , FALSE,5
                             , ((load_thread_data *)(tmpnode->data))->loadfile 
                             , ((load_thread_data *)(tmpnode->data))->sqlldr_cmd);

            plog(FUL_EXEC_LOG,"\n",FALSE,4);
        
            tmpnode = tmpnode->next;
         } /* end while */

      }/* end if */

   } /* end if */


   /* close log file */
   fclose(logfd);

   cleanup(FUL_CLEAN_CODE_SUCCESS);

   return EXIT_SUCCESS;
}

/* --------------------------------------------------- 
 * this is the function that does most of the job.
 * if something to be optimized it should be that 
 * function.
 * --------------------------------------------------- */
void unload( OCI_Thread *thread_id, void * utd )
{   
 
   FILE *unloadfiled; /* unload file/pipe descriptor */
   OCI_Statement* stmt;  
   OCI_Resultset* rs;
   register unsigned short int i = 0;
   register char **delimiter;
   time_t start_time,end_time;

   /* this saves a cmp time for each column and a print 
    * function call for each record */
   /*uqmd.record->col_cnt = 1;*/
   delimiter = (char **)safe_malloc(1 * sizeof(char *));

   for( i = 0; i < uqmd.record->col_cnt-1; i++)
      asprintf(&(delimiter[i]),"%s",uv.field_delimiter) ;
   
   asprintf(&(delimiter[i]),"%s",uv.record_delimiter) ;
   
   stmt = OCI_StatementCreate(con);
   
   OCI_SetFetchSize(stmt, uv.fetch_size); 


   if(stmt == NULL)
   {
      plog(FUL_EXEC_LOG, "Failed to create statement !\n"
                         "Quitting ...\n\n", TRUE,1 );
      cleanup( FUL_CLEAN_CODE_FAILURE );
      return;
   }

   unloadfiled = fopen((char *)((unload_thread_data *)utd)->unloadfile,"w");
   
   
   time (&start_time);
   asprintf(&(((unload_thread_data *)utd)->ulog->start_time) ,get_local_time());
   

   /* Execute the statement */
   OCI_ExecuteStmt(stmt,(char *)((unload_thread_data *)utd)->unload_query );
    
   
   /* Get the result set */ 
   rs = OCI_GetResultset(stmt);
   


   while ( OCI_FetchNext(rs) ) 
   {  
      for(i = 1; i <= uqmd.record->col_cnt; i++ )
      {
         /* ------------------------------------------------------------
          * TODO : can OCI_* functions in here be inlined ? 
          * TODO : find optimum i/o block size 
          *        add buffering and replace fprintf with write syscall
          * ------------------------------------------------------------ */         
	
         fprintf(unloadfiled ,  "%s%s"
                             ,  OCI_IsNull(rs, i) ? "" : OCI_GetString(rs,i)
                             ,  delimiter[i-1] );
	
	 fprintf(stdout ,  "%s\n",(char *)OCI_GetString(rs,i));
	 fprintf(stdout ,  "%s\n","kerti");	
      }	 
   }
      printf("colcnt %d",uqmd.record->col_cnt);	

   fflush(unloadfiled);
   fclose(unloadfiled);
   
   time (&end_time);
   ((unload_thread_data *)utd)->ulog->elapsed_time  = difftime(end_time, start_time);
   asprintf(&( ((ul_logs *)((unload_thread_data *)utd)->ulog)->end_time) , get_local_time());

   /* number of unloaded records for this thread */ 
   ((ul_logs *)(((unload_thread_data *)utd)->ulog))->row_count 
      = (unsigned long int)OCI_GetRowCount ( rs );

   /* clean up */
   OCI_ReleaseResultsets (stmt);
}

/* --------------------------------------------------- 
 * loads the data to the given target
 * --------------------------------------------------- */
void load( OCI_Thread *thread_id, void * ltd )
{
   time_t start_time,end_time;

   time (&start_time);
   asprintf(&( ((ul_logs *)((load_thread_data *)ltd)->llog)->start_time) , get_local_time());

#if defined(FULL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_SQLLDR_COMMAND)
   plog(FUL_DEBUG_LOG, "Executing: %s\n\n"
                     , ((load_thread_data *)ltd)->sqlldr_cmd
                     , TRUE,0);
#endif


   /* execute command sh -c */
   ((load_thread_data *)ltd)->sqlldr_ret_code = 
      system(((load_thread_data *)ltd)->sqlldr_cmd);
   
   /* check for return code */
   switch(((load_thread_data *)ltd)->sqlldr_ret_code)
   {
      case FUL_SQLLDR_EX_SUCC : break;
      case FUL_SQLLDR_EX_FAIL :
                                plog(FUL_EXEC_LOG, "Command line/syntax errors | Oracle errors fatal to SQL*Loader !\n"
                                                   "SQL*Loader returned with EX_FAIL!\n"
                                                   "Consult to log file %s\n\n"
                                                 , TRUE,1, ((load_thread_data *)ltd)->loadfile);
                                break;
      case FUL_SQLLDR_EX_WARN :
                                plog(FUL_EXEC_LOG, "All/some rows rejected | All/some rows discarded | Discontinued load !\n"
                                                   "SQL*Loader returned with EX_WARN!\n"
                                                   "Consult to log file %s\n\n"
                                                 , TRUE,1, ((load_thread_data *)ltd)->loadfile);
                                break;                                      
      case FUL_SQLLDR_EX_FTL  :
                                plog(FUL_EXEC_LOG, "OS related errors (like file open/close, malloc, etc.)!\n"
                                                   "SQL*Loader returned with EX_FTL!\n"
                                                   "Consult to log file %s\n\n"
                                                 , TRUE,1, ((load_thread_data *)ltd)->loadfile);
                                break;                                                               
   }


   time (&end_time);
   
   ((load_thread_data *)ltd)->llog->elapsed_time  = difftime(end_time, start_time);
   asprintf(&( ((ul_logs *)((load_thread_data *)ltd)->llog)->end_time) , get_local_time());

}

/* --------------------------------------------------- 
 * plog function is used for logging. this function is
 * similar to printf function but does not accempts as
 * many qualifiers as printf.the arguments in here are;
 * log_type - FUL_DEBUG_LOG|FUL_EXEC_LOG
 * logstr   - log string
 * ptime    - TRUE|FALSE (whether print time or not)
 * log_level- ( -5] log level  
 * --------------------------------------------------- */
void plog(int log_type, char *logstr, bool ptime, int log_level, ...) 
{
   va_list  arg_ptr; /* argument pointer */
   char *lsp;        /* log string pointer */
   char *tmpstr;     /* temp. string */

   /* always print if debugging */
   if( (log_type == FUL_DEBUG_LOG && o.debugging) ||
       (log_type == FUL_EXEC_LOG && log_level <= o.log_level) ) {
      
      if(ptime) fprintf(logfd,"%s - ", rtrim(get_local_time(),"\n"));

      /* prepare variable list */
      va_start( arg_ptr, log_level );

      for( lsp = logstr; *lsp; ++lsp ) 
      {
         
         if( *lsp != '%') 
            fprintf(logfd ,"%c",*lsp); 
         else  
            switch( *(++lsp) ) 
            {
               case 's' : /* string */
                  tmpstr = va_arg( arg_ptr, char * );
                  fprintf( logfd,"%s", tmpstr?tmpstr:"NULL" );
                  continue;
               case 'd' : /* integer */
                  fprintf( logfd,"%d",va_arg( arg_ptr, int ) );
                  continue;
               case 'i' : /* integer */
                  fprintf( logfd,"%i",va_arg( arg_ptr, int ) );
                  continue;
               case 'u' : /* unsigned integer */
                  fprintf( logfd,"%u",va_arg( arg_ptr, unsigned int ) );
                  continue;   
               case 'c' : /* character */
                  fprintf( logfd,"%c",va_arg( arg_ptr, int ) );
                  continue;
               case 'l' : /* %ld long */
                  if(*(++lsp) == 'f')
                     fprintf( logfd,"%.2lf",va_arg( arg_ptr, double ) );
                  else if(*lsp == 'd' )
                     fprintf( logfd,"%ld",va_arg( arg_ptr, long ) );
                  continue;
               default : putc( *lsp,logfd );      
            }
            
      }

      va_end( arg_ptr );
      fflush(logfd); 
   }
   
   
}

/* --------------------------------------------------- 
 * sets log file descriptor. if log file is not given
 * then log output is directed standard output 
   --------------------------------------------------- */
void setlogfd( void ) 
{

   if(o.log_file == NULL) { logfd = (FILE *)stdout; return; }

   logfd = fopen (o.log_file, "w");

   if(logfd == NULL) 
   {
      logfd = (FILE *)stdout;
      plog(FUL_EXEC_LOG , "Failed to set log file. Redirecting to stdout..."
                        , TRUE,1);
   }

}

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_ROWID_RANGES)	
/* --------------------------------------------------- 
 * this function prints rowid ranges
 * --------------------------------------------------- */
void printRowIdRanges( void )
{
   

   ListNode *tmpnode = (ListNode *)NULL;

   plog(FUL_EXEC_LOG,
            "\n----------------------------------------------------------\n"
            "  Rowid Ranges\n"
            "----------------------------------------------------------\n\n"
            ,FALSE,1);

   plog(FUL_DEBUG_LOG , "Table Owner: %s\n"
                        "Table Name : %s\n\n" 
                      , FALSE,0
                      , uqmd.mt->owner
                      , uqmd.mt->name ); 



   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE )
   {
      printRowIdInfo( uqmd.mt->rowidinfo );        
   }
   
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION )
   {
      tmpnode = uqmd.mt->part_list->head;
   
      while( tmpnode != NULL)
      {
         plog(FUL_DEBUG_LOG , "Partition Name : %s\n"
                           , FALSE,0
                           , ((partition *)(tmpnode->data))->name); 
      
         printRowIdInfo( (rowid_info *)(((partition *)(tmpnode->data))->rowidinfo) );       
      
         tmpnode = tmpnode->next;   
      }
      
   }

   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION )
   {
      tmpnode = uqmd.mt->subpart_list->head;
   
      while( tmpnode != NULL)
      {
         plog(FUL_DEBUG_LOG, "Subpartition Name : %s\n"
                           , FALSE,1
                           , ((subpartition *)(tmpnode->data))->name); 
      
         printRowIdInfo( (rowid_info *)(((subpartition *)(tmpnode->data))->rowidinfo) );        
      
         tmpnode = tmpnode->next;   
      }
   }

   plog(FUL_EXEC_LOG,"\n",FALSE,1);   

}

/* --------------------------------------------------- 
 * this function prints the given rowid information
 * --------------------------------------------------- */
void printRowIdInfo( rowid_info *rowidinfo )
{
   register unsigned short int i ;


   for( i = 0; i < rowidinfo->rowid_range_count; i++)
   {
       plog(FUL_DEBUG_LOG, "%d   %s          %s\n"
                         , FALSE,0
                         , i+1
                         , (char *)(rowidinfo->rowid_from[i])
                         , (char *)(rowidinfo->rowid_to[i]) );
   }

   plog(FUL_DEBUG_LOG, "\n", FALSE,0 );
   
}
#endif

/* --------------------------------------------------- 
 * set fetch size is a beta function and aims to
 * find best fetch size for an unload session. This
 * function is in a beta stage and not used for now.
 * --------------------------------------------------- */
#ifdef FUL_ENABLE_AUTO_FETCH_SIZE

void setFetchSize( ) 
{
   struct sysinfo sys_info;
   long freeram;
   long pagesize;
   long avphy_pages;

   if( sysinfo(&sys_info) )
   {
      plog(FUL_EXEC_LOG , 
            "First try failed to find free memory! Using a different method ...\n"
            ,TRUE,4);

         if( (pagesize = sysconf(_SC_PAGESIZE)) != -1 &&
             (avphy_pages = sysconf(_SC_AVPHYS_PAGES)) != -1)
         {
            freeram = pagesize*avphy_pages;
            uv.fetch_size = (int)((float)FUL_MAGIC_RATIO*(freeram / uqmd.record->max_rec_len));
         }
         else
         {
            plog(FUL_EXEC_LOG , "Failed to set fetch size! Using default %d.\n"
                              , TRUE,4
                              , FUL_DEFAULT_FETCH_SIZE);
         
            uv.fetch_size = FUL_DEFAULT_FETCH_SIZE;
         }
   }
   else
      uv.fetch_size = (int)((float)FUL_MAGIC_RATIO*(sys_info.freeram / uqmd.record->max_rec_len));
   
}
#endif

/* --------------------------------------------------- 
 * this function handles the errors that is generated
 * by the OCI.
 * --------------------------------------------------- */
void ErrorHandler(OCI_Error *err)
{

   plog(FUL_EXEC_LOG,
                     "\ncode  : ORA-%i\n"
                     "msg   : %s\n"
                     "sql   : %s\n"
                     "Qiting ...\n\n"
                   , TRUE,0
                   , OCI_ErrorGetOCICode(err)
                   , OCI_ErrorGetString(err)
                   , OCI_GetSql(OCI_ErrorGetStatement(err))
           );
 
    cleanup(FUL_CLEAN_CODE_FAILURE);
    exit(1);  
}

/* --------------------------------------------------- 
 * returns the proper sql statement to obtain the
 * row id range information from the server.
 * --------------------------------------------------- */
char * getRowIdSql( char * object_name )
{
	char *rrq;
   char *ao_fltr = ""; /* all objects filter */
   char *de_fltr = ""; /* dba extends filter */

   if ( object_name != NULL ) 
      if (asprintf(&ao_fltr,"AND SUBOBJECT_NAME = UPPER('%s') ",object_name ) < 0 || 
          asprintf(&de_fltr,"AND PARTITION_NAME = UPPER('%s') ",object_name ) < 0 )
      {        
         plog(FUL_EXEC_LOG, "Unable to allocate memory for RowidRange query\n"
                            "Quiting ...\n\n" 
                          , TRUE,1);

         cleanup(FUL_CLEAN_CODE_FAILURE);
         exit(1);
      }
		

	if( asprintf( &rrq,"SELECT DBMS_ROWID.rowid_create (1,\n"
                                "data_object_id,\n"
                                "lo_fno,\n"
                                "lo_block,\n"
                                "0) rowid_from,\n"
       "DBMS_ROWID.rowid_create (1,\n"
                                "data_object_id,\n"
                                "hi_fno,\n"
                                "hi_block,\n"
                                "10000\n) rowid_to \n"
		"FROM (WITH c1 AS \n"
             "(SELECT   * \n"
                  "FROM dba_extents \n"
                 "WHERE segment_name = UPPER ('%s') \n"
                   "AND owner = UPPER ('%s') %s \n" 
              "ORDER BY block_id) \n"
        "SELECT DISTINCT grp, \n"
                        "FIRST_VALUE (relative_fno) OVER (PARTITION BY grp ORDER BY relative_fno, \n"
                        "block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo_fno, \n"
                        "FIRST_VALUE (block_id) OVER (PARTITION BY grp ORDER BY relative_fno, \n"
                        "block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo_block, \n"
                        "LAST_VALUE (relative_fno) OVER (PARTITION BY grp ORDER BY relative_fno, \n"
                        "block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi_fno, \n"
                        "LAST_VALUE (block_id + blocks - 1) OVER (PARTITION BY grp ORDER BY relative_fno, \n"
                        "block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi_block, \n"
                        "SUM (blocks) OVER (PARTITION BY grp) AS sum_blocks \n"
                   "FROM (SELECT   relative_fno, block_id, blocks, \n"
                                  "TRUNC(  (  SUM (blocks) OVER (ORDER BY relative_fno,block_id)- 0.01) "
                                        "/ (SUM (blocks) OVER () / %d) ) grp \n"
                             "FROM c1 \n"
                            "WHERE segment_name = UPPER ('%s') \n"
                            "AND owner = UPPER ('%s') \n"
                         "ORDER BY block_id)),\n"
			"(SELECT data_object_id \n"
			"FROM all_objects \n"
			"WHERE object_name = UPPER ('%s') AND owner = UPPER ('%s') %s)"
                                                      , uqmd.mt->name  ,   uqmd.mt->owner
                                                      , de_fltr        ,   uv.par_degree
                                                      , uqmd.mt->name  ,   uqmd.mt->owner
                                                      , uqmd.mt->name  ,   uqmd.mt->owner
                                                      , ao_fltr) < 0)
      {        
         plog(FUL_EXEC_LOG, "Unable to allocate memory for RowidRange query\n"
                            "Quiting ...\n\n" 
                          , TRUE,1);
         
               cleanup(FUL_CLEAN_CODE_FAILURE);
               exit(1);
      }

	return rrq;
}

/* --------------------------------------------------- 
 * parses the configuration file and sends the attribute
 * value pairs to another function that matches these
 * tuples
 * --------------------------------------------------- */
void parseConfigFile( const char *config_file ) 
{ 
   
   bool SET_OPT = FALSE;
   bool SET_CMT = FALSE;
   bool SET_OPTARG = FALSE;

   const char SPACE_CHAR = ' ';
   const char CR_CHAR   = '\n';
   const char LF_CHAR   = '\r';  
   const char TAB_CHAR  = '\t';

   char option[FUL_MAX_OPTION_LEN];
   char optarg[FUL_MAX_OPTARG_LEN];
   char readbuffer[FUL_LINE_BUFFER_SIZE];
   FILE *configfiled = NULL;  
   register unsigned int i = 0;
   unsigned int optind    = 0;
   unsigned int optargind = 0;
   int mark01 = 0;
   char *tmpstr = optarg;
  
   configfiled = fopen( config_file , "r" );
   if (configfiled == NULL) 
      fatal("Unable to open configuration file.\n");  
   
   memset(option,'\0',FUL_MAX_OPTION_LEN);
   memset(optarg,'\0',FUL_MAX_OPTARG_LEN);

   while( NULL != fgets(readbuffer,FUL_LINE_BUFFER_SIZE,configfiled) ) 
   {
      
      for(i = 0; readbuffer[i] != '\0' && i <= FUL_LINE_BUFFER_SIZE ; i++)
      {
         if( SET_OPT && 
             (readbuffer[i] != SPACE_CHAR) &&
             (readbuffer[i] != CR_CHAR))  
         {

            if(optind > FUL_MAX_OPTION_LEN )
            {
               plog(FUL_EXEC_LOG, "Error in configuration file! Check the argument lengths."
                                  "Quiting ...\n\n"
                                , TRUE,0 );

               cleanup(FUL_CLEAN_CODE_FAILURE);
            }      
            option[optind++] = readbuffer[i];
         }
         if(SET_OPTARG && !SET_CMT && readbuffer[i] != '$' && readbuffer[i] != '#')
         {  
            if(readbuffer[i] != CR_CHAR && 
               readbuffer[i] != SPACE_CHAR) mark01 = optargind; 
            
                        
            if(optargind > FUL_MAX_OPTARG_LEN )
            {
               plog(FUL_EXEC_LOG, "Error in configuration file! Check the argument lengths.\n"
                                   "Quiting ...\n\n"
                                ,  TRUE,0 );

               cleanup(FUL_CLEAN_CODE_FAILURE);
            }      
            optarg[optargind++] = readbuffer[i]; 
         }

         if(readbuffer[i] == '#') 
         {   
           SET_CMT = TRUE; /* comment */
           SET_OPT = FALSE;
         }  
         if(readbuffer[i] == '$' && !SET_CMT ) /* option */
         {
            SET_OPT = TRUE;
            SET_CMT = SET_OPTARG = FALSE;

            if(*optarg != '\0' && *option != '\0')
            {   
               for(optargind=0; (optargind != mark01+1) &&
                   (optarg[optargind] == SPACE_CHAR || 
                    optarg[optargind] == CR_CHAR    ||
                    optarg[optargind] == TAB_CHAR   || 
                    optarg[optargind] == LF_CHAR); optargind++) tmpstr++; 
               
               optarg[mark01+1] = '\0';
               setopt(option, tmpstr);
               optargind = optind = 0;
               memset(option,'\0',FUL_MAX_OPTION_LEN);
               memset(optarg,'\0',FUL_MAX_OPTARG_LEN);
               mark01 = 0; tmpstr = optarg;
               SET_OPTARG = SET_CMT = FALSE;
            }
         }
         if(SET_OPT && (readbuffer[i] == CR_CHAR || 
                        readbuffer[i] == SPACE_CHAR))   
         {
            SET_OPT = SET_CMT = FALSE;
            SET_OPTARG = TRUE;
         }
         if(SET_CMT && (readbuffer[i] == CR_CHAR) )   
            SET_CMT = FALSE;
      } 

      memset(readbuffer,'\0',FUL_LINE_BUFFER_SIZE);     
   }
    
   if(SET_OPTARG)
   {
      for(optargind=0; (optargind != mark01+1) && 
          (optarg[optargind] == SPACE_CHAR || 
           optarg[optargind] == CR_CHAR    ||
           optarg[optargind] == TAB_CHAR   ||
           optarg[optargind] == LF_CHAR); optargind++) tmpstr++; 
      
      optarg[mark01+1] = '\0';      
      setopt(option,tmpstr);
   }

   fclose(configfiled);
}

/* ------------------------------------------------- *
 * this function sets the options from the command   * 
 * line interface. if configuration file is given in *
 * here then program first sets the options using    *
 * configuration file then overrides those options   *
 * with the ones that are given in command line      *
 * ------------------------------------------------- */
void setCommandLineOptions( int argc, char **fakeargv ) 
{

   int  opt;
   int  option_index = 0;
   char * tmpstr ;
   extern char *optarg;

   while (( opt =  getopt_long_only (argc, fakeargv
                                    , ":c:N:u:p:m:o:n:a:l:P:S:D:t:f:v:V:x:X:Y:g:e:d:U:Q:G:q:T:O:Z:z:F:K:"
                                    , long_options, &option_index) ) != -1) 
   {

      tmpstr = strdup(optarg);
      switch(opt)
      {
         case 'c' : o.config_file      = tmpstr; break; /* configuration file    */
         case 'N' : o.net_service_name = tmpstr; break; /* net service name      */
         case 'u' : o.user_name    = tmpstr; break;     /* user name             */
         case 'p' : o.password     = tmpstr; break;     /* password              */  
         case 'm' : o.con_ses_mode = tmpstr; break;     /* connect session mode  */
         case 'o' : o.mtab_owner       = tmpstr; break; /* main table schema           */
         case 'n' : o.mtab_name        = tmpstr; break; /* main table name             */
         case 'a' : o.mtab_allias      = tmpstr; break; /* main table allias           */
         case 'l' : o.mtab_part_level  = tmpstr; break; /* main table partition level  */ 
         case 'P' : if(tmpstr) o.partlist    = tmpstr; else uv.set_partlist   = TRUE; break; /* comma separated partition list    */ 
         case 'S' : if(tmpstr) o.subpartlist = tmpstr; else uv.set_subpartlist= TRUE; break; /* comma separated subpartition list */ 
         case 'D' : o.par_degree       = tmpstr; break; 
         case 't' : o.par_type         = tmpstr; break; 
         case 'f' : o.unload_file      = tmpstr; break; /* unload file/pipe name       */   
         case 'v' : o.field_delimiter  = tmpstr; break; /* field delimiter             */
         case 'V' : o.record_delimiter = tmpstr; break; /* record delimiter            */
         case 'x' : o.xfield_delimiter = tmpstr; break; /* hex field delimiter         */
         case 'X' : o.xrecord_delimiter= tmpstr; break; /* hex record delimiter        */
         case 'Y' : o.date_format      = tmpstr; break; /* date format                 */           
         case 'g' : o.fetch_size       = tmpstr; break; /* raw fetch size              */
         case 'e' : o.log_level        = isNumber(tmpstr) ? abs(atoi(tmpstr)) : FUL_DEFAULT_LOG_LEVEL; break;           
         case 'd' : o.debugging        = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE;   break;
         case 'U' : o.unload2pipe      = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE;   break;
         case 'Q' : o.unload_query_file= tmpstr; break; /* unload query file  */
         case 'G' : o.gen_ctl_file     = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE;   break; /* generate ctl file  */
         case 'q' : o.unload_query     = tmpstr; break; /* unload query       */             
         case 'T' : o.load_data        = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE; break; /* call sql loader */
         case 'O' : o.sqlldr_cmd       = tmpstr;  break;    /* sql*loader command with additional options */     
         case 'Z' : o.target_table_owner = tmpstr;  break;  /* target owner */
         case 'z' : o.target_table_name  = tmpstr;  break;  /* target name  */
         case 'F' : o.ctl_file_name      = tmpstr;  break;  /* sql loader control file name  */           
         case 'K' : o.truncate_target_table = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE ; break; /* truncate target table */
         case '?' : printUsage(1); break; /* print usage information */
         default : printUsage(1); break;          
      }
   }

}

/* --------------------------------------------------- 
 * initialize options structure
 * --------------------------------------------------- */
void initOptions() {
   
   o.user_name = NULL;
   o.password  = NULL;
   o.debugging = FALSE;
   o.log_level = 0;
   o.unload2pipe = FALSE;
   o.gen_ctl_file = FALSE;
   o.load_data = FALSE;
   o.partlist = NULL;
   o.subpartlist = NULL;
   o.fetch_size = NULL;
   o.xfield_delimiter = NULL;
   o.xrecord_delimiter = NULL;
   o.field_delimiter = NULL;
   o.record_delimiter = NULL;
   o.con_ses_mode  = NULL;
   o.ctl_file_name = NULL;
   o.mtab_allias = NULL; 
   o.truncate_target_table = FALSE;
}


/* --------------------------------------------------- 
 * initializes the unload variables with default values
 * ---------------------------------------------------  */
void InitUnloadVariables(void)
{

   uv.mtx                  = NULL;
   uv.par_degree           = 1;
   uv.par_type             = FUL_PARALLEL_TYPE_NONE;
   uv.fetch_size           = FUL_DEFAULT_FETCH_SIZE;
   uv.ulog.row_count       = 0;
   uv.ulog.elapsed_time         = 0;
   uv.ulog.serial_elapsed_time  = 0;
   uv.llog.serial_elapsed_time  = 0;
   uv.set_partlist         = FALSE;
   uv.set_subpartlist      = FALSE;
   uv.unloadstream_type    = FUL_UNLOAD_STREAM_TYPE_FILE;
   (uv.field_delimiter)    = NULL;
   (uv.record_delimiter)   = NULL;
}

/* --------------------------------------------------- 
 * setopt function sets the argument value pairs.
 * some of the arguments set here are further
 * processed later on and stored in the unload
 * variables structure.
 * --------------------------------------------------- */
void setopt(char *opt, char *optarg) 
{

   char * tmpstr = NULL;

   if(optarg[0] != '\0') tmpstr = trim( rtrim(strdup(optarg),"\r\n") );
    
  /* 
   printf("\nOptArgSize : %d\n"
          "Option     : %s\n"
          "OptArg     : %s\n",sizeof(tmpstr),opt, tmpstr);
   
   */
   if(!strcasecmp(opt, "NET_SERVICE_NAME"))  o.net_service_name = tmpstr;           /* set net service name */ 
   else if(!strcasecmp(opt,"USER_NAME"))     o.user_name = tmpstr;                  /* set connect username */
   else if(!strcasecmp(opt,"PASSWORD"))      o.password = tmpstr;                   /* set password         */
   else if(!strcasecmp(opt,"TABLE_OWNER"))   o.mtab_owner = tmpstr;                 /* set main table sch   */
   else if(!strcasecmp(opt,"TABLE_NAME"))    o.mtab_name = tmpstr;                  /* set main table name  */
   else if(!strcasecmp(opt,"TABLE_ALLIAS"))  o.mtab_allias = tmpstr;                /* set main allias      */
   else if(!strcasecmp(opt,"TABLE_PARTITION_LEVEL"))  o.mtab_part_level = tmpstr;   /* set main table parititon level  */
   else if(!strcasecmp(opt,"PARTITION_NAME"))      if(tmpstr) o.partlist = tmpstr;  else uv.set_partlist = TRUE;        /* set partition names     */ 
   else if(!strcasecmp(opt,"SUBPARTITION_NAME"))   if(tmpstr) o.subpartlist = tmpstr;  else uv.set_subpartlist = TRUE;  /* set subpartition names  */ 
   else if(!strcasecmp(opt,"PARALLEL_DEGREE"))     o.par_degree = tmpstr;           /* set parallel degree  */
   else if(!strcasecmp(opt,"PARALLEL_TYPE"))       o.par_type = tmpstr;             /* parallelisation type */
   else if(!strcasecmp(opt,"FETCH_SIZE"))          o.fetch_size = trim(tmpstr);     /* set fetch size    */
   else if(!strcasecmp(opt,"FIELD_DELIMITER"))     o.field_delimiter = tmpstr;      /* set field sep.       */ 
   else if(!strcasecmp(opt,"RECORD_DELIMITER"))    o.record_delimiter = tmpstr;     /* set record sep.      */
   else if(!strcasecmp(opt,"HEX_FIELD_DELIMITER"))    o.xfield_delimiter = tmpstr;  /* hex. fild sep.       */  
   else if(!strcasecmp(opt,"HEX_RECORD_DELIMITER"))   o.xrecord_delimiter = tmpstr; /* hex. record sep      */
   else if(!strcasecmp(opt,"DATE_FORMAT"))            o.date_format = tmpstr;       /* date format          */
   else if(!strcasecmp(opt,"UNLOAD_FILE_NAME"))       o.unload_file = tmpstr;       /* unload file name     */
   else if(!strcasecmp(opt,"EXEC_LOG_FILE"))      o.log_file = tmpstr;              /* execution log file   */
   else if(!strcasecmp(opt,"EXEC_LOG_LEVEL"))     o.log_level = isNumber(tmpstr)==TRUE ? abs(atoi(tmpstr)) : FUL_DEFAULT_LOG_LEVEL; /* execution log level */
   else if(!strcasecmp(opt,"DEBUGGING"))          o.debugging = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE ; /* debugging         */
   else if(!strcasecmp(opt,"UNLOAD_QUERY_FILE"))  o.unload_query_file  = tmpstr;                            /* unload query file */
   else if(!strcasecmp(opt,"UNLOAD_QUERY"))       o.unload_query = strdup(tmpstr);                          /* unload query      */
   else if(!strcasecmp(opt,"UNLOAD_TO_PIPE")) o.unload2pipe = !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE ;   /* unload to pipe    */
   else if(!strcasecmp(opt,"CONNECT_SESSION_MODE")) o.con_ses_mode = strdup(tmpstr);
   else if(!strcasecmp(opt,"GENERATE_CTL_FILE")) o.gen_ctl_file =  !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE ; /* generate ctl file */
   else if(!strcasecmp(opt,"LOAD_DATA"))         o.load_data =  !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE ;    /* generate ctl file */
   else if(!strcasecmp(opt,"SQLLDR_COMMAND")) o.sqlldr_cmd =  tmpstr;                                          /* addiitional sql loader options   */
   else if(!strcasecmp(opt,"TARGET_TABLE_OWNER")) o.target_table_owner       =  tmpstr;                        /* target table owner to load data  */
   else if(!strcasecmp(opt,"TARGET_TABLE_NAME")) o.target_table_name         =  tmpstr;                        /* target table name to load data   */
   else if(!strcasecmp(opt,"TRUNCATE_TARGET_TABLE")) o.truncate_target_table =  !strcasecmp(tmpstr,"TRUE") ? TRUE : FALSE; /* truncate target table */
   else if(!strcasecmp(opt,"SQLLDR_CTL_FILE"))   o.ctl_file_name  =  tmpstr; /* user given ctl file */

}


/* --------------------------------------------------- 
 * this function stores the partition list that is
 * given by the user.
 * --------------------------------------------------- */
bool setPartitionList( void ) 
{
   ListNode *node = NULL;
   partition *part_info;
   char *tmpstr;
   int slen,toklen = 0;
   char * tok;
 
   tmpstr = strdup(o.partlist);

   if( uqmd.mt->part_list ) ListFree( uqmd.mt->part_list );

   uqmd.mt->part_list = ListCreate();
   
   slen = strlen(tmpstr);
   
   if(tmpstr[slen]-1 == '\n')
      tmpstr[slen-1] = '\0';
   
   tok = trim(strtok(tmpstr, ","));
   while (tok != NULL) 
   {
      toklen = strlen(tok);
      if(toklen > FUL_MAX_PARTITION_NAME_LEN)
      {
         plog(FUL_EXEC_LOG, "Maximum partition name length exceeded\n"
                            "Maximum partition name length is %s\n"
                            "%s has %d characters!\n\n"
                          , TRUE,1
                          , FUL_MAX_PARTITION_NAME_LEN
                          , tok,toklen);
         return FALSE;
      }

      part_info = (partition *)safe_malloc(sizeof(partition));

      asprintf( &(part_info->name), (char *)tok );
      part_info->rowidinfo = (rowid_info *)NULL;
      part_info->ulog = (ul_logs *)NULL;

      node = (ListNode *)ListAppend((LinkedList *)uqmd.mt->part_list
                                    , sizeof(partition *));
      node->data = (void *)part_info;    
      tok = trim(strtok(NULL,","));
   }
   
   /* cleanup */
   free(tmpstr);
   return TRUE;
}

/* --------------------------------------------------- 
 * this function stores the subpartition list that is
 * given by the user.
 * --------------------------------------------------- */
bool setSubPartitionList( void ) 
{
   
   ListNode *node = NULL;
   subpartition *subpart_info;
   char *tmpstr;
   int slen,toklen = 0;
   char * tok;
 
   tmpstr = strdup(o.partlist);

   if( uqmd.mt->subpart_list ) ListFree( uqmd.mt->subpart_list );

   uqmd.mt->subpart_list = ListCreate();
   
   slen = strlen(tmpstr);
   
   if(tmpstr[slen]-1 == '\n') tmpstr[slen-1] = '\0';
   
   tok = trim(strtok(tmpstr, ","));
   while (tok != NULL) 
   {
      toklen = strlen(tok);
      if(toklen > FUL_MAX_SUBPARTITION_NAME_LEN)
      {
         plog(FUL_EXEC_LOG, "Maximum subpartition name length exceeded\n"
                            "Maximum subpartition name length is %s\n"
                            "%s has %d characters!\n\n"
                          , TRUE,1
                          , FUL_MAX_SUBPARTITION_NAME_LEN
                          , tok,toklen);
         return FALSE;
      }

      subpart_info = (subpartition *)safe_malloc(sizeof(partition));

      asprintf( &(subpart_info->name), (char *)tok );
      subpart_info->rowidinfo = NULL;
      subpart_info->ulog = NULL;

      node = (ListNode *)ListAppend((LinkedList *)uqmd.mt->subpart_list
                                    , sizeof(subpartition *));
      node->data = (void *)subpart_info;    

      tok = trim(strtok(NULL,","));
   }

   /* cleanup */
   free(tmpstr);
   return TRUE;

}

/* --------------------------------------------------- 
 * if partition list argument is left blak than funloader
 * retrieves the list of all partitions that the main table
 * has and stores them in the related data structure.
 * --------------------------------------------------- */
bool setDBPartitionList(OCI_Statement * stmt) 
{
   char *sql_stmt;
   ListNode *node = NULL;
   OCI_Resultset* rs;
   partition *part_info; 

   if( uqmd.mt->part_list ) ListFree( uqmd.mt->part_list );
    
   uqmd.mt->part_list = (LinkedList *)ListCreate();

   asprintf( &sql_stmt,
               "SELECT PARTITION_NAME FROM ALL_TAB_PARTITIONS\n"
               "WHERE TABLE_NAME = UPPER('%s') AND\n"
               "TABLE_OWNER = UPPER('%s')"
             , uqmd.mt->name,uqmd.mt->owner );
    
   if( !OCI_ExecuteStmt(stmt, sql_stmt) || 
       !( rs = OCI_GetResultset(stmt) )  )  
   { 
      free(sql_stmt); 
      ListFree(uqmd.mt->part_list); 
      return FALSE; 
   }
   while ( OCI_FetchNext(rs) ) 
   {
      part_info = (partition *)safe_malloc(sizeof(partition));
      
      asprintf( &(part_info->name), (char *)OCI_GetString(rs,1) );
      part_info->rowidinfo = (rowid_info *)NULL;
      part_info->ulog = (ul_logs *)NULL;
      
      node = (ListNode *)ListAppend((LinkedList *)uqmd.mt->part_list
                                    , sizeof(partition *));
      
      node->data = (void *)part_info;         
   }   
   
   free(sql_stmt);

   return TRUE;
}

/* --------------------------------------------------- 
 * if partition list argument is left blak than funloader
 * retrieves the list of all subpartitions that the 
 * main table has and stores them in the related 
 * data structure.
 * --------------------------------------------------- */
bool setDBSubPartitionList(OCI_Statement * stmt) 
{
   char *sql_stmt;
   ListNode *node = NULL;
   OCI_Resultset* rs;
   subpartition *subpart_info; 
   

   if( uqmd.mt->subpart_list ) ListFree( uqmd.mt->part_list );
   
   uqmd.mt->subpart_list = (LinkedList *)ListCreate();


   asprintf( &sql_stmt,
               "SELECT SUBPARTITION_NAME FROM ALL_TAB_SUBPARTITIONS\n"
               "WHERE TABLE_NAME = UPPER('%s') AND\n"
               "TABLE_OWNER = UPPER('%s')"
             , uqmd.mt->name,uqmd.mt->owner );
    
   if( !OCI_ExecuteStmt(stmt, sql_stmt) || 
       !( rs = OCI_GetResultset(stmt) )  )  
   { 
      free(sql_stmt);
      ListFree(uqmd.mt->subpart_list);
      return FALSE; 
   }

   while ( OCI_FetchNext(rs) ) 
   {
      subpart_info = (subpartition *)safe_malloc(sizeof(subpartition));
      
      asprintf( &(subpart_info->name), (char *)OCI_GetString(rs,1) );
      subpart_info->rowidinfo = (rowid_info *)NULL;
      subpart_info->ulog = (ul_logs *)NULL;
      
      node = (ListNode *)ListAppend((LinkedList *)uqmd.mt->subpart_list
                                    , sizeof(subpartition *));
      
      node->data = (void *)subpart_info;  
   }   
   
   /* clean up */
   free(sql_stmt);
   return TRUE;
}

/* --------------------------------------------------- 
 * this function prints the names of paritions that
 * that is processed by funloader.
 * --------------------------------------------------- */
void printPartitionInfo( void ) 
{   
   ListNode *tmpnode = NULL; 

   tmpnode = uqmd.mt->part_list->head;

   while(tmpnode != NULL)
   {
      plog(FUL_DEBUG_LOG,  "Partition %d : %s\n"
                        ,  FALSE, 4
                        ,  (char *)((partition *)tmpnode->data)->name );
      
      tmpnode = tmpnode->next;
   }

   plog(FUL_DEBUG_LOG,"\n\n",FALSE,0);

}

/* --------------------------------------------------- 
 * this function prints the names of paritions that
 * that is processed by funloader.
 * --------------------------------------------------- */
void printSubPartitionInfo( void ) 
{   
   ListNode *tmpnode = NULL; 

   tmpnode = uqmd.mt->subpart_list->head;

   while(tmpnode != NULL)
   {
      plog(FUL_DEBUG_LOG,  "Subpartition %d : %s\n"
                        ,  FALSE, 4 
                        ,  (char *)((subpartition *)tmpnode->data)->name );

      tmpnode = tmpnode->next;
   }

   plog(FUL_DEBUG_LOG,"\n\n",FALSE,0);

}

/* --------------------------------------------------- 
 * this function simply prints the contents of unload
 * variables
 * --------------------------------------------------- */
void printUnloadVariables( void )
{
   /* debugging */
   plog(FUL_DEBUG_LOG,
                       "\n-*-------------------------------------------*-\n"
                       "Unload Variables\n"
                       "-*-------------------------------------------*-\n"
                       "PARALLEL_DEGREE ............. : %d\n" 
                       "PARALLEL_TYPE ............... : %d\n"
                       "FIELD_DELIMITER ............. : %s\n"
                       "RECORD_DELIMITER ............ : %s\n"
                       "PREFETCH_SIZE *.............. : %d\n" 
                       "SET_SUBPARTITION_LIST ....... : %s\n"
                       "SET_PARTITION_LIST .......... : %s\n"
                       "UNLOAD_FILE_TYPE ............ : %s\n"
                     , FALSE, 0
                     , uv.par_degree, uv.par_type, uv.field_delimiter
                     , uv.record_delimiter, uv.fetch_size
                     , uv.set_subpartlist ? "TRUE" : "FALSE"
                     , uv.set_partlist ? "TRUE" : "FALSE"
                     , uv.unloadstream_type == FUL_UNLOAD_STREAM_TYPE_FILE 
                       ? "FILE" : "PIPE");                       
}

/* --------------------------------------------------- 
 * prints the unload variables that is given by the
 * user.
 * --------------------------------------------------- */
void printOptions() 
{
   /* debugging */
   plog(FUL_DEBUG_LOG,
                       "\n-*-------------------------------------------*-\n"
                       "Options\n"
                       "-*-------------------------------------------*-\n"
                       "CONFIG_FILE ................. : %s\n"
                       "NET_SERVICE_NAME ............ : %s\n"
                       "USER_NAME ................... : %s\n"
                       "PASSWORD .................... : %s\n"
                       "CONNECT_SESSION_MODE ........ : %s\n"
                       "TABLE_OWNER ................. : %s\n"
                       "TABLE_NAME .................. : %s\n"
                       "TABLE_ALLIAS ................ : %s\n"
                       "TABLE_PARTITION_LEVEL ....... : %s\n"
                       "PARTITION_NAME *............. : %s\n"  
                       "SUBPARTITION_NAME *.......... : %s\n"
                       "PARALLEL_DEGREE ............. : %s\n"
                       "PARALLEL_TYPE ............... : %s\n"
                       "UNLOAD_FILE_NAME *........... : %s\n"  
                       "FIELD_DELIMITER ............. : %s\n"
                       "RECORD_DELIMITER ............ : %s\n"
                       "HEX_FIELD_DELIMITER ......... : %s\n"
                       "HEX_RECORD_DELIMITER ........ : %s\n"
                       "DATE_FORMAT ................. : %s\n"
                       "FETCH_SIZE *................. : %s\n"
                       "EXEC_LOG_LEVEL .............. : %d\n"
                       "EXEC_LOG_FILE ............... : %s\n"
                       "DEBUGGING ................... : %s\n"
                       "UNLOAD_TO_PIPE .............. : %s\n"
                       "UNLOAD_QUERY_FILE ........... : %s\n"
                       "GENERATE_CTL_FILE ........... : %s\n"
                       "LOAD_DATA ................... : %s\n"
                       "TARGET_TABLE_OWNER .......... : %s\n"
                       "TARGET_TABLE_NAME ........... : %s\n" 
                       "TRUNCATE_TARGET_TABLE ....... : %s\n\n"
                     , FALSE,0 
                     , o.config_file, o.net_service_name, o.user_name, o.password 
                     , o.con_ses_mode
                     , o.mtab_owner, o.mtab_name, o.mtab_allias
                     , o.mtab_part_level, o.partlist, o.subpartlist,o.par_degree, o.par_type
                     , o.unload_file, o.field_delimiter, o.record_delimiter
                     , o.xfield_delimiter, o.xrecord_delimiter, o.date_format
                     , o.fetch_size,  o.log_level, o.log_file, o.debugging ? "TRUE":"FALSE" 
                     , o.unload2pipe ? "TRUE" : "FALSE" , o.unload_query_file
                     , o.gen_ctl_file ? "TRUE" : "FALSE", o.load_data ? "TRUE" : "FALSE" 
                     , o.target_table_owner,o.target_table_name
                     , o.truncate_target_table ? "TRUE" : "FALSE");  

}

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_SYS_INFO) 
/* --------------------------------------------------- 
 * prints the system information and mainly used for 
 * debugging purposes
 * --------------------------------------------------- */
void printSysInfo( void )
{
   struct sysinfo sys_info;
   int days,hours,mins;
   long secs;   

   if( sysinfo(&sys_info) != 0 )
      plog( FUL_EXEC_LOG , "Unable to get sysinfo\n\n" ,FALSE,5);
   else
   {
      days  = sys_info.uptime  / 86400;   
      hours = (sys_info.uptime / 3600) - (days * 24);
      mins  = (sys_info.uptime / 60) - (days * 1440) - (hours * 60);
      secs  = sys_info.uptime % 60;

      plog(FUL_EXEC_LOG, 
                     "\n"
                     "+ ---------------------------------------------------------- +\n"
                     "+ System Information                                         +\n"
                     "+ ---------------------------------------------------------- +\n\n"
                     "System Up Time: %ddays, %dhours, %dminutes, %ldseconds\n"
                     "Shared Ram: %ld KB\n"
                     "Buffered Ram: %ld KB\n"
                     "Number of Running Processes: %d\n\n" 
                   , TRUE,5
                   , days,hours,mins,secs
                   , sys_info.sharedram / 1024
                   , sys_info.bufferram / 1024
                   , sys_info.procs); 
   }
}

#endif

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_DB_VERSION) 
/* --------------------------------------------------- 
 * prints the database version information
 * --------------------------------------------------- */
void printDBVersion( OCI_Connection *cn)
{

   plog(FUL_EXEC_LOG,
                  "\n"
                  "+ ---------------------------------------------------------- +\n"
                  "+ DB Server Information                                      +\n"
                  "+ ---------------------------------------------------------- +\n\n"
                  "Server major    version: %i\n"
                  "Server minor    version: %i\n" 
                  "Server revision version: %i\n\n" 
                  "Connection      version: %i\n\n" 
                                          ,  TRUE,0
                                          ,  OCI_GetServerMajorVersion(cn)
                                          ,  OCI_GetServerMinorVersion(cn)
                                          ,  OCI_GetServerRevisionVersion(cn)
                                          ,  OCI_GetVersionConnection(cn)); 
}

#endif
/* --------------------------------------------------- *
 *  cleans the handles and relases the resources.      *
 *  if used mutex objects also should be freed in      *
 *  here.                                              *
 * --------------------------------------------------- */
void cleanup( unsigned short int clean_code )
{  
   /* --------------------------------------------
   The following objects are freed by OCI_Cleanup :
   - Connections
   - Connection pools
   - Statements
   - Schema objects
   - Thread keys
   The objects that are not listed above, should
   also be freed in this function
    --------------------------------------------*/

   if(clean_code == FUL_CLEAN_CODE_FAILURE )
      plog(FUL_EXEC_LOG,"Program terminated !\n\n",TRUE,1);
   else
      plog(FUL_EXEC_LOG,"Program ended successfully\n\n",TRUE,1);

   if( uv.mtx ) OCI_MutexFree ( uv.mtx );
   
   OCI_Cleanup();
   
   if(clean_code == FUL_CLEAN_CODE_FAILURE )
      exit(FUL_CLEAN_CODE_FAILURE);

}

/* --------------------------------------------------- 
 * this function sets the partition level of the main
 * table using the information retrieved from database
 * server.
 * --------------------------------------------------- */
void setPartitionLevel( OCI_Statement * stmt )
{
   char *sql_stmt;
   OCI_Resultset* rs;
   int cnt;

   asprintf( &sql_stmt,
               "SELECT COUNT(1) FROM ALL_TAB_SUBPARTITIONS\n"
               "WHERE TABLE_NAME = UPPER('%s') AND\n"
               "TABLE_OWNER = UPPER('%s')", uqmd.mt->name, uqmd.mt->owner );

   OCI_ExecuteStmt(stmt, sql_stmt);
   rs = OCI_GetResultset(stmt);
   OCI_FetchNext(rs);
   cnt = OCI_GetInt(rs,1);
   
   free(sql_stmt);

   if( cnt > 0 ) 
   {/* main table has sub partitions */
      uqmd.mt->part_level = FUL_PARTITION_LEVEL_SUBPARTITION;
   }
   else
   { /* check if main table has partitions */
      asprintf( &sql_stmt,
               "SELECT COUNT(1) FROM ALL_TAB_PARTITIONS\n"
               "WHERE TABLE_NAME = UPPER('%s') AND\n"
               "TABLE_OWNER = UPPER('%s')", uqmd.mt->name, uqmd.mt->owner );

      OCI_ExecuteStmt(stmt, sql_stmt);
      rs = OCI_GetResultset(stmt);
      OCI_FetchNext(rs);
      cnt = OCI_GetInt(rs,1);
      if(cnt > 0)
      {/* main table has sub partitions */
         uqmd.mt->part_level = FUL_PARTITION_LEVEL_PARTITION;      
      }
      else
         uqmd.mt->part_level = FUL_PARTITION_LEVEL_NONE;      
   }
      
   /* clean up */
   OCI_ReleaseResultsets (stmt);
   free(sql_stmt);
}


/* --------------------------------------------------- 
 * generates control file. some of the options are
 * supplied in here but they can be overridden 
 * during the loading process.
 * --------------------------------------------------- */
void generateCtlFile( void )
{
   char *ctlfname ;
   FILE *ctlfd;
   register unsigned short int i;
   
   asprintf(&ctlfname,"%s.ctl",o.unload_file);
   ctlfd = fopen(ctlfname,"w");
   


   
   fprintf(ctlfd, "OPTIONS (\n"
	               "SKIP=0,\n"
	               "ERRORS=0,\n"
	               "DIRECT=TRUE,\n"
	               "PARALLEL=TRUE\n"
	               ")\n"
                  "LOAD DATA\n"
                  "CHARACTERSET WE8ISO8859P9\n");
   
   fprintf(ctlfd, "INFILE \"\" \"STR '%s'\""
                  "BADFILE \"%s.bad\"\n" 
                  "DISCARDFILE \"%s.dsc\"\n" 
                  "DISCARDMAX 1\n"
                  "APPEND\n"
                  "INTO TABLE %s.%s\n" 
                  "FIELDS TERMINATED BY '%s'\n" 
                  "TRAILING NULLCOLS\n"
                  "(\n"
                , o.record_delimiter  
                , o.unload_file,  o.unload_file 
                , o.target_table_owner
                , o.target_table_name
                , o.field_delimiter );

   
   for( i=0; i < uqmd.record->col_cnt; i++ )
   {

      fprintf( ctlfd , "  %s  "
                     , uqmd.record->col[i]->name);


      if(!strcasecmp(uqmd.record->col[i]->sqltype,"CHAR") ||
         !strcasecmp(uqmd.record->col[i]->sqltype,"VARCHAR2") )
      {

         fprintf( ctlfd ,  "  %s(%u)"
                        ,  "CHAR"
                        ,  ( unsigned int )uqmd.record->col[i]->size);
      }
      else if(!strcasecmp(uqmd.record->col[i]->sqltype,"DATE") ||
              !strcasecmp(uqmd.record->col[i]->sqltype,"TIMESTAMP") )
      {
        
         fprintf(ctlfd , " %s \"%s\""
                       , uqmd.record->col[i]->sqltype
                       , o.date_format);
      }
      
      fprintf(ctlfd , " %s\n"
                    , i != uqmd.record->col_cnt-1 ? "," : ""  );

   }

   fprintf( ctlfd , ")\n");

   fflush(ctlfd);
   fclose(ctlfd);
}

/* --------------------------------------------------- * 
 * this function sets field separator                  *  
 * or record separator                                 *
 * --------------------------------------------------- */
void setDelimiter(const char *src, char **dest, unsigned short int delimiter_type )
{
   register unsigned short int i ;
   unsigned short int slen =0;

   slen  = strlen(src);
   *dest = (char *)safe_malloc( (slen +1)*sizeof(char) ) ;
   unsigned int dec = 0;

   for( i = 0; i < slen; i++)
   {
      switch( *(src +i) )
      {
         case '\\' :
            if( *(src +i+1) == 'n' ) /* new line */
            {  
               *(*dest + i) = '\n'; i++; dec++;
            }  
            else if( *(src +i+1) == 'r' ) /* carriage return */
            {
               *(*dest + i) = '\r'; i++; dec++;
            }
            else if( *(src +i+1) == 't' ) /* tab */
            {
               *(*dest + i) = '\t'; i++; dec++;
            }  
            else *(*dest + i) = '\\';
            break;
         default  :
            *(*dest + i) = *(src +i);
            break;
      }
   } 
   
   *(*dest + i-dec) = '\0';
}

/* --------------------------------------------------- 
 * converts a given hex character string to equivelent
 * digit in base ten
 * --------------------------------------------------- */
int getHexDigit(const char x)
{
   
   if ( x >='0' && x <='9') return x - '0';
   if ( x >='A' && x <='F') return 10 + (x - 'A');
   if ( x >='a' && x <='f') return 10 + (x - 'a');
   
   return 0;
}

/* ------------------------------------ *
 * this function checks for the special *
 * characters that can not be set as a  *
 * separator                            *
 * ------------------------------------ */
bool checkAscii(const int chrval )
{
   /* .........................
    * check out the ascii table 
    * for details 
    * ......................... */

   if( (chrval >= 0  && chrval <= 8 ) || 
       (chrval >= 11 && chrval <= 12) ||
       (chrval >= 14 && chrval <= 31) ||
       (chrval == 127) )
      return FALSE ;
      
   return TRUE;
}

/* ------------------------------------------------------------- 
 * this function sets hexadecimal field separator 
 * or record separator
 * ------------------------------------------------------------- */
void setXDelimiter(const char *src, char **dest, unsigned short int delimiter_type )
{

   register unsigned short int i ;
   unsigned short int j = 0;
   const unsigned short int slen = strlen(src);

   if( slen%2 )
   {
      plog(FUL_EXEC_LOG , "Invalid hex string is given for \"%s\".\n"
                          "Hex values should be in two digit format\n\n"  
                        , FALSE, 1  
                        , delimiter_type==FUL_RECORD_DELIMITER ? "record delimiter":"field delimiter");
      
      *dest = NULL;
      return; 
   }

   *dest = (char *)safe_malloc( (slen/2 +1)*sizeof(char) ) ;

   for( i = 0; i < slen; i+=2)
   {
      *(*dest + j) = getHexDigit(*(src + i+1)) + (getHexDigit(*(src +i)) << 4);
      
      if( checkAscii(*(*dest + j)) == FALSE )
      { 
         plog(FUL_EXEC_LOG , "Invalid hex character is given for \"%s\".\n\n"
                           , FALSE, 1
                           , delimiter_type==FUL_RECORD_DELIMITER ? "record delimiter":"field delimiter");

         free(*dest);
         *dest = NULL;
         return;
      }
      j++;
   }
   
   *(*dest + j) = '\0';
}


/* ------------------------------------------
 * This function returns a string that 
 * identical to the given string except the
 * invisible characters replaced by their 
 * matching string representations.
 * Example: "fun loader" -> "fun[SPACE]loader" 
 *
 * ------------------------------------------ */
char * getVisibleString(const char *strval)
{
   /* .........................
    * check out the ascii table 
    * for details 
    * ......................... */

   const unsigned short int CR_SZ    =4;
   const unsigned short int LF_SZ    =4;
   const unsigned short int SPACE_SZ =7;
   const unsigned short int TAB_SZ   =5;
  
   register unsigned int i = 0;
   unsigned int buffer_size = sizeof(strval);
   char *buffer;
   char buffer_ind = 0;

   while(strval[i] != '\0' && strval != NULL) 
   {
      switch((int)strval[i])
      {
         case  9 : buffer_size += TAB_SZ;   break;
         case 10 : buffer_size += CR_SZ;    break;
         case 13 : buffer_size += LF_SZ;    break;
         case 32 : buffer_size += SPACE_SZ; break;         
      }
      i++;
   }
   
   if(buffer_size == sizeof(strval))
      return (char *)strval;
   
   buffer = (char *)safe_malloc(buffer_size);

   memset(buffer,'\0',buffer_size);

   while(strval[i] != '\0' && strval ) 
   {
      switch((int)strval[i])
      {
         case  9 : strcat(buffer, "[TAB]"); 
                   buffer_ind += TAB_SZ; 
                   break;
         case 10 : strcat(buffer, "[LF]");  
                   buffer_ind += LF_SZ;   
                   break;
         case 13 : strcat(buffer, "[CR]");  
                   buffer_ind += CR_SZ; 
                   break;
         case 32 : strcat(buffer, "[SPACE]"); 
                   buffer_ind += SPACE_SZ; 
                   break;
         default : *(buffer+buffer_ind) = *(strval+i);
                   buffer_ind++;         
                   break;
      }
      i++;
   }
   
   return buffer;   
}

/* --------------------------------------------------- 
 * divides the main table to rowid ranges and stores
 * them in related data structure
 * --------------------------------------------------- */
void setRowIdRanges( OCI_Statement * stmt )
{
   char *sql_stmt = (char *)NULL; /* row id sql statement */
   ListNode *tmpnode;   

   OCI_SetFetchMode(stmt, OCI_SFM_SCROLLABLE);
   
   if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_NONE )
   {      
      sql_stmt = getRowIdSql( NULL );
      uqmd.mt->rowidinfo = (rowid_info *)getRowIdInfo(stmt,sql_stmt);
   }
   else if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_PARTITION )
            
   {
      tmpnode = uqmd.mt->part_list->head;

      while( tmpnode != NULL)
      {
         sql_stmt = getRowIdSql( ( (partition *) tmpnode->data)->name );
         ((partition *)((ListNode *)tmpnode)->data)->rowidinfo = (rowid_info *)getRowIdInfo(stmt,sql_stmt);         
      
         tmpnode = tmpnode->next;
      }   
   }
   else if( uqmd.mt->part_level == FUL_PARTITION_LEVEL_SUBPARTITION )
   {
      tmpnode = uqmd.mt->subpart_list->head;

      while( tmpnode != NULL)
      {
         sql_stmt = getRowIdSql( ( (subpartition *) tmpnode->data)->name );
         ((subpartition *)((ListNode *)tmpnode)->data)->rowidinfo = (rowid_info *)getRowIdInfo(stmt,sql_stmt);         
      
         tmpnode = tmpnode->next;
      }
   }
   else
   {
      plog(FUL_EXEC_LOG, "Unexpected error occured during setting rowid ranges\n"
                         "Quitting ...\n\n"       
                       , TRUE,1);

      cleanup(FUL_CLEAN_CODE_FAILURE);
      exit(1);
   }
   
   free(sql_stmt);
}

/* --------------------------------------------------- 
 * retrieves the rowid information from database server
 * --------------------------------------------------- */
rowid_info * getRowIdInfo( OCI_Statement * stmt, char *sql_stmt )
{
   OCI_Resultset* rs;
   rowid_info *rowidinfo;   
   unsigned short int rowid_cnt; /* actual rowid range count */  
   register unsigned short int i = 0;
   
   OCI_ExecuteStmt( stmt, sql_stmt );
   rs = OCI_GetResultset(stmt);
   OCI_FetchLast(rs);

   rowid_cnt = OCI_GetRowCount(rs);

   OCI_FetchFirst(rs);
   
   rowidinfo = (rowid_info *)safe_malloc(sizeof(rowid_info));    

   rowidinfo->rowid_from = (char **)safe_malloc(rowid_cnt*sizeof(char *));
   rowidinfo->rowid_to   = (char **)safe_malloc(rowid_cnt*sizeof(char *));   
   rowidinfo->rowid_range_count = rowid_cnt;

   do{   

      rowidinfo->rowid_from[i] = (char *)safe_malloc(OCI_SIZE_ROWID+1);
      rowidinfo->rowid_to[i]   = (char *)safe_malloc(OCI_SIZE_ROWID+1);

      strcpy( rowidinfo->rowid_from[i], OCI_GetString(rs,1) );
      strcpy( rowidinfo->rowid_to[i], OCI_GetString(rs,2) );
            
      i++;
   }while( OCI_FetchNext(rs) );

   OCI_ReleaseResultsets(stmt);
   
   return rowidinfo;
}

/* ---------------------------------------------------
 *  sets the record related table metadata
 * --------------------------------------------------- */

void setRecordInfo(OCI_Statement * stmt)
{
   OCI_Resultset* rs;
   OCI_Column *ocicol;
   register unsigned short int i = 0;

   OCI_SetFetchMode(stmt, OCI_SFM_DEFAULT); 
   
   OCI_ExecuteStmt(stmt, uqmd.unloadquery );
   rs = OCI_GetResultset(stmt);

   uqmd.record = (record_info *)safe_malloc(sizeof(record_info));   
   
   uqmd.record->max_rec_len = 0;
   uqmd.record->col_cnt = OCI_GetColumnCount(rs);

   uqmd.record->col = 
      (column_info **)safe_malloc(uqmd.record->col_cnt*sizeof(column_info *));

   for( i=0; i != uqmd.record->col_cnt; i++ )
   {
      ocicol = (OCI_Column *)OCI_GetColumn(rs, i+1);
      
      uqmd.record->col[i] = (column_info *)safe_malloc(sizeof(column_info));

      asprintf(&(uqmd.record->col[i]->name),   (char *)OCI_ColumnGetName (ocicol) ); 
      asprintf(&(uqmd.record->col[i]->sqltype),(char *)OCI_ColumnGetSQLType (ocicol) );
      uqmd.record->col[i]->charused = OCI_ColumnGetCharUsed (ocicol);
      uqmd.record->col[i]->size = OCI_ColumnGetSize (ocicol);
      uqmd.record->max_rec_len += uqmd.record->col[i]->size;   
   }   

   OCI_ReleaseResultsets (stmt);
}

/* ---------------------------------------------------
 * this function tries to extract certain information
 * such as main table allias and name positions
 * in the given unload query string. This is a very
 * trivial function should not be considered as sql
 * statement parsing. sql parsing is far away from
 * trivial and not in the scope for now
 * --------------------------------------------------- */
void prepareUnloadQueryReWrite( void )
{
  
   /* FIXME this function should be rewriteen
    * it works now for straigth cases 
    * */


   char *mark01 = NULL; /* used to keep position in str */
   char *mark02 = NULL; /* used to keep position in str */ 
   char *longtablename = NULL;
   unsigned short int i = 0;
   unsigned short int j = 0;
   unsigned short int owner_name_len = 0;
   unsigned short int table_name_len = 0;

   /* .......................................
    * fulltablename can be either
    * in the format table_owner.table_name
    * or table_name 
    * ....................................... */
   

   owner_name_len = strlen(uqmd.mt->owner);
   table_name_len = strlen(uqmd.mt->name);

   uv.query_part_pos  = NULL;
   uv.query_has_where = FALSE;
   
   /* narrow down the query block */
   mark01 = strstr(strupper(&uqmd.unloadquery), " FROM " );
   mark02 = strstr(mark01, " WHERE "); 

   mark01++;/*space*/ 
   if(mark02)
   { 
      mark02++;/*space*/ 
      uv.query_has_where = TRUE;
   }
   else   
      uv.query_has_where = FALSE;
      
   if( o.mtab_allias && uv.par_type == FUL_PARALLEL_TYPE_ROWID )
   {
      asprintf(&(uqmd.mt->allias), o.mtab_allias);
      return;
   }

   /* connect username is different from 
    * main table owner */
   if( strcasecmp(o.user_name,uqmd.mt->owner) != 0 )
   {
      
      asprintf(&longtablename , "%s.%s"
                              , uqmd.mt->owner
                              , uqmd.mt->name);  
      
      mark01 = strstr(mark01,strupper(&longtablename));
      mark01 += (owner_name_len+table_name_len+1);
             
   }
   else 
   {     
         SEARCH_FORWARD:  

         mark01 = strstr(mark01, uqmd.mt->name);

         if( *(mark01-1) == '.' )
         {   
            for(j= 0; j < owner_name_len && 
                      *(mark01-j-2) == *(uqmd.mt->owner + owner_name_len-j-1)
                    ; j++ ) ;
           
            if( j > 0 && j != owner_name_len )
            {
               
               mark01 += 1;
               goto SEARCH_FORWARD;

            }
            else if( j== owner_name_len )
            {  
               mark01 += table_name_len;
            }
                
         }
         else mark01 += table_name_len+1;        
         
   }
   if( uv.par_type == FUL_PARALLEL_TYPE_ROWID )
   {
      uqmd.mt->allias = (char *)safe_malloc(FUL_MAX_TABLE_ALLIAS_LEN);
      memset(uqmd.mt->allias,'\0',FUL_MAX_TABLE_ALLIAS_LEN);
      
      
      i = 0;
      while(*mark01 != '\0' && mark01 && 
            *mark01 != ','  && mark01 != mark02)
      {
         if( (*mark01 != ' ') && (*mark01 != ')') && *mark01 != '\n'  )
         {  
            *(uqmd.mt->allias+i) = *mark01; 
            i++;
         }
         mark01++;
      }
      if( i > 0 )
         *(uqmd.mt->allias+i) = '\0';
      else 
      {  
         free(uqmd.mt->allias);
         uqmd.mt->allias = strdup(uqmd.mt->name);

      }

   }
   else if ( uv.par_type == FUL_PARALLEL_TYPE_PARTITION ||
             uv.par_type == FUL_PARALLEL_TYPE_SUBPARTITION )
   {
      uv.query_part_pos = mark01;
      
     /* while(mark01 != '\0' && mark01 && 
            *mark01 != ',' && mark01 != mark02)
      {
         printf("mark01 %s\n",mark01);
         printf("mark02 %s\n",mark02);
         if( *mark01 == ' ' || *mark01 == '\t'||
             *mark01 == '\n'|| *mark01 == '\0' )
         {
            uv.query_part_pos = mark01;
            break;
         }
         mark01++;
      }*/
   }
   
   free(longtablename);
}

/* --------------------------------------------------- 
 * this function rewrites the unload query string
 * so that it has the rowid or partition filtering
 * depending on the unload configuration.
 * --------------------------------------------------- */
void UnloadQueryReWrite(   const char *rowid_from, const char *rowid_to
                         , const char *object_name
                         , char **unload_query )
{

   char *longobjectname = NULL;

   if( uv.par_type == FUL_PARALLEL_TYPE_ROWID )
   {
      if(uv.query_has_where) /* has WHERE */
      {
         
         /* " AND .rowid BETWEEN '' AND ''" 30 */
         (*unload_query) =
            (char *)safe_malloc(  strlen(uqmd.unloadquery)
                                + (OCI_SIZE_ROWID<<1)
                                + strlen(uqmd.mt->allias)
                                + 30 + 1 );

         
      
         asprintf(unload_query,  "%s AND %s.rowid BETWEEN '%s' AND '%s'"
                              ,  uqmd.unloadquery,uqmd.mt->allias
                              ,  rowid_from,rowid_to );

      }
      else 
      {
         /* " WHERE .rowid BETWEEN '' AND ''" 32 */
         (*unload_query) =
            (char *)safe_malloc(  strlen(uqmd.unloadquery)
                                + (OCI_SIZE_ROWID<<1)
                                + strlen(uqmd.mt->allias)
                                + 32 +1 );
        
         asprintf(unload_query,  "%s WHERE %s.rowid BETWEEN '%s' AND '%s'"
                              ,  uqmd.unloadquery, uqmd.mt->allias
                              ,  rowid_from,rowid_to );
      }
   }
   else if( uv.par_type == FUL_PARALLEL_TYPE_PARTITION )
   {
      asprintf(&longobjectname," PARTITION(%s) ",object_name);
      (*unload_query) = strinsert(uqmd.unloadquery, uv.query_part_pos, longobjectname);
   }
   else if( uv.par_type == FUL_PARALLEL_TYPE_SUBPARTITION )
   {
      asprintf(&longobjectname," SUBPARTITION(%s) ",object_name);
      (*unload_query) = strinsert(uqmd.unloadquery, uv.query_part_pos, longobjectname);

   }

   free(longobjectname);

/* .....................................................................  
" AND (uqmd->mt->allias).rowid BETWEEN '(rowid_from)' AND '(rowid_to)'"   
" AND .rowid BETWEEN '' AND ''" 32
" WHERE .rowid BETWEEN '' AND ''" 34
*/

}

/* --------------------------------------------------- 
 * reads the given unload query and stores the contents
 * in unload query variable.
 * --------------------------------------------------- */
bool  readUnloadQueryFile( void )
{
   FILE *uqfiled;
   unsigned long file_size;
   unsigned long read_size;
   unsigned register int i = 0;

   uqfiled = fopen(o.unload_query_file,"r");

   if(uqfiled == NULL)
   {

      plog(FUL_EXEC_LOG , "Failed to open unload query file %s\n"
                        , TRUE ,1
                        , o.unload_query_file);

      return FALSE;     
   }
    
   fseek (uqfiled, 0, SEEK_END);
   file_size = (unsigned long)ftell(uqfiled);
   rewind(uqfiled);
   
   if(file_size < FUL_MIN_UQ_FILE_SIZE  )
   {
      plog(FUL_EXEC_LOG, "Unload query file size is less than minumum size %d !\n"
                       , TRUE,1
                       , FUL_MIN_UQ_FILE_SIZE);   
      
      return FALSE;
   }       
      
   uqmd.unloadquery = (char *)safe_malloc(file_size*sizeof(char));
              
   read_size = fread(uqmd.unloadquery, 1, file_size, uqfiled);

   if(read_size != file_size )
   {
      plog(FUL_EXEC_LOG, "Unload query file read error !\n"
                       , TRUE,1);

      free(uqmd.unloadquery);
      return FALSE;
   }    

   while(1)
   {  /* refine the given unload query */
      if(uqmd.unloadquery[read_size-i] == '\n' ||
         uqmd.unloadquery[read_size-i] == '\r' ||
         uqmd.unloadquery[read_size-i] == '\t' ||
         uqmd.unloadquery[read_size-i] == ' '  ||
         uqmd.unloadquery[read_size-i] == '\0')
            uqmd.unloadquery[read_size-i] = '\0';
      else
         break;         

      i++;
   }

   return TRUE;

}

/* --------------------------------------------------- 
 * initializes sql loader command string
 * --------------------------------------------------- */
void setSqlldrCmd( unsigned short int thread_no, char **sqlldr_cmd)
{

   if(o.ctl_file_name)
      asprintf(sqlldr_cmd , "%s control=%s log=%s_%03d.log bad=%s_%03d.bad data=%s_%03d > /dev/null"
                          , o.sqlldr_cmd , o.ctl_file_name
                          , o.unload_file, thread_no
                          , o.unload_file, thread_no
                          , o.unload_file, thread_no);
   else
      asprintf(sqlldr_cmd , "%s control=%s.ctl log=%s_%03d.log bad=%s_%03d.bad data=%s_%03d > /dev/null"
                          , o.sqlldr_cmd , o.unload_file
                          , o.unload_file, thread_no
                          , o.unload_file, thread_no
                          , o.unload_file, thread_no);
}


#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_LOAD_THREAD_DATA)
/* --------------------------------------------------- 
 * prints the data that is sent to the unloading thread.
 * basically used for debugging purposes.
 * --------------------------------------------------- */
void printUnloadThreadData( unload_thread_data * utd)
{
   
   plog(FUL_DEBUG_LOG, 
                   "-------------------------------\n"
                   "session Type : Unload"
                   "Session No   : %d\n"
                   "Unload File  : %s\n"
                   "Unload Query : %s\n\n"
                   "-------------------------------\n\n"
                 , FALSE,0
                 , utd->thread_no
                 , utd->unloadfile
                 , utd->unload_query );   

}
#endif

#if defined(FUL_DEBUG_AGGRESSIVE) || defined(FUL_DEBUG_UNLOAD_THREAD_DATA)
/* --------------------------------------------------- 
 * prints the data that is sent to the loading thread.
 * basically used for debugging purposes.
 * --------------------------------------------------- */
void printLoadThreadData( load_thread_data * ltd)
{
   
   plog(FUL_DEBUG_LOG, 
                   "-------------------------------\n"
                   "Session Type       : Load"
                   "Session No         : %d\n" 
                   "Data File          : %s\n"
                   "SqlLoader Command  : %s\n\n"
                   "-------------------------------\n\n"
                 , FALSE,0
                 , ltd->thread_no
                 , ltd->loadfile
                 , ltd->sqlldr_cmd );   

}

#endif
