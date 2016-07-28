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


#ifndef FUL_OPTIONS_H
#define FUL_OPTIONS_H


   #include "getopt.h"




const struct option long_options[] =
             {
               {"CONFIG_FILE"             ,  required_argument,   0, 'c'}, /* configuration file                  */
               {"NET_SERVICE_NAME"        ,  required_argument,   0, 'N'}, /* src net service name                */
               {"USER_NAME"               ,  required_argument,   0, 'u'}, /* src user name                       */
               {"PASSWORD"                ,  required_argument,   0, 'p'}, /* src password                        */
               {"CONNECT_SESSION_MODE"    ,  required_argument,   0, 'm'}, /* src connect session mode            */
               {"TABLE_OWNER"             ,  required_argument,   0, 'o'}, /* source main table owner             */ 
               {"TABLE_NAME"              ,  required_argument,   0, 'n'}, /* source main table name              */
               {"TABLE_ALLIAS"            ,  required_argument,   0, 'a'}, /* source main table allias            */
               {"TABLE_PARTITION_LEVEL"   ,  required_argument,   0, 'l'}, /* source main table partition level   */   
               {"PARTITION_NAME"          ,  optional_argument,   0, 'P'}, /* partition name(s)                   */
               {"SUBPARTITION_NAME"       ,  optional_argument,   0, 'S'}, /* subpartition name(s)                */
               {"PARALLEL_DEGREE"         ,  required_argument,   0, 'D'}, /* parallel degree                     */
               {"PARALLEL_TYPE"           ,  required_argument,   0, 't'}, /* parallel type                       */ 
               {"UNLOAD_FILE_NAME"        ,  required_argument,   0, 'f'}, /* unload file name                    */
               {"FIELD_DELIMITER"         ,  required_argument,   0, 'v'}, /* field separator                     */
               {"RECORD_DELIMITER"        ,  required_argument,   0, 'V'}, /* record separator                    */
               {"HEX_FIELD_DELIMITER"     ,  required_argument,   0, 'x'}, /* field separator hex                 */
               {"HEX_RECORD_DELIMITER"    ,  required_argument,   0, 'X'}, /* record separator hex                */
               {"DATE_FORMAT"             ,  required_argument,   0, 'Y'}, /* date format                         */
               {"FETCH_SIZE"              ,  required_argument,   0, 'g'}, /* prefetch size                       */
               {"EXEC_LOG_LEVEL"          ,  required_argument,   0, 'e'}, /* execution log level                 */
               {"EXEC_LOG_FILE"           ,  required_argument,   0, 'L'}, /* log filename                        */
               {"DEBUGGING"               ,  required_argument,   0, 'd'}, /* debugging                           */
               {"UNLOAD_TO_PIPE"          ,  required_argument,   0, 'U'}, /* unload to named pipe (unix only)    */
               {"UNLOAD_QUERY_FILE"       ,  required_argument,   0, 'Q'}, /* unload query file                   */
               {"UNLOAD_QUERY"            ,  required_argument,   0, 'q'}, /* unload query                        */
               {"GENERATE_CTL_FILE"       ,  required_argument,   0, 'G'}, /* generate sql loader ctl file        */
               {"LOAD_DATA"               ,  required_argument,   0, 'T'}, /* load data to target table           */
               {"SQLLDR_OPT_STRING"       ,  required_argument,   0, 'O'}, /* additional sql loader options       */
               {"TARGET_TABLE_OWNER"      ,  required_argument,   0, 'Z'}, /* owner of the target table           */
               {"TARGET_TABLE_NAME"       ,  required_argument,   0, 'z'}, /* name of the target table            */
               {"TRUNCATE_TARGET_TABLE"   ,  required_argument,   0, 'K'}, /* truncate target table               */
               {"SQLLDR_CTL_FILE"         ,  required_argument,   0, 'F'}, /* sql loader control file name        */

               {0, 0, 0, 0}
             };



#endif

