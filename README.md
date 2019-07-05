#!/bin/bash
#Generic Ingestion Script For UBS
#################################################################################
# 
#
# v20.0 - 2015-10-09 - Final Version
#################################################################################
# This is Generic Ingestion Script for tables in all the layers
#
# Return Codes:
#    0   - successful execution
#    101 - a required command line input is either missing or invalid
#    102 - an unrecognized input switch was provided
#    103 - could not create temporary log file
#    104 - a required variable was not defined in the INI_FILE
#    105 - could not remove an old log file
#    106 - the Lock file (LOCK_FILE) is not writable
#    107 - the Lock file (LOCK_FILE) already exists.  A previous scheduled
#          processes is either still running or has failed mid-stream
#    112 - the Log Directory (LOG_DIR) does not exist
#    113 - the archive subdirectory of the Log Directory (LOG_DIR/archive) does not exist
#    114 - the Log Directory (LOG_DIR) is not writable
#    115 - the archive subdirectory of the Log Directory (LOG_DIR/archive) is not writable"
#    116 - could not remove LOCK_FILE after completion
#    117 - could not create LOCK_FILE after initializations
#    124 - INI file does not exist
#    125 - referenced SQL file does not exist
#    126 - HIVE or IMPALA returned a non-zero exit code
#    127 - Copy from dropbox to Hadoop returned a non-zero exit code
#    128 - Move files Dropbox to Backup returned a non-zero exit code
#    129 - Deletion of files from Backup returned a non-zero exit code
#    130 - Folder consists of Zero Bytes Files
#    131 - Deletion of files returned a non-zero exit code
#    132 - Multiple DATA_AS_OF_DATE or DATA_AS_OF_DATE is not same as SYSTEM_DATE
#    133 - Backup Directory does not exists. 
#    134 - Hive Table doesnot exists.
#    135 - Issue while Kerberos Authentication .
#    136 - Input File Doesnot Exist ."
#    137 - Orcestartion process fail.
#################################################################################
# Define all the functions (in the future, potential to use fpath)
#set -x

function PROGRAM_USAGE(){
   echo
   echo "Usage:  ./Ingest_File.sh -i [INI_FILE] -s [INI_SECTION]"
   echo "   -i is required.  This is the fully qualified (pathed) INI filename"
   echo "   -s is required.  This is the section in the INI file pertaining to"
   echo "                    the specific execution.  Values are case specific"
   echo
   echo "Example:"
   echo "   /full/path/Ingest_File.sh -i /full/path/something.ini -s SECTION"
   echo
   echo "Return Codes:"
   echo "   0   - sucessful execution"
   echo "   101 - a required command line input is either missing or invalid"
   echo "   102 - an unrecognized input switch was provided"
   echo "   103 - could not create temporary log file"
   echo "   104 - a required variable was not defined in the INI_FILE"
   echo "   105 - could not remove an old log file"
   echo "   106 - the Lock file (LOCK_FILE) is not writable"
   echo "   107 - the Lock file (LOCK_FILE) already exists.  A previous scheduled"
   echo "         process is either still running or has failed mid-stream"
   echo "   112 - the Log Directory (LOG_DIR) does not exist"
   echo "   113 - the archive subdirectory of the Log Directory (LOG_DIR/archive) does not exist"
   echo "   114 - the Log Directory (LOG_DIR) is not writable"
   echo "   115 - the archive subdirectory of the Log Directory (LOG_DIR/archive) is not writable"
   echo "   116 - could not remove LOCK_FILE after SFTP completion"
   echo "   117 - could not ceate LOCK_FILE after initializations"
   echo "   124 - INI file does not exist"
   echo "   125 - referenced SQL file does not exist"
   echo "   126 - HIVE or IMPALA returned a non-zero exit code"
   echo "   127 - Copy from Dropbox to Hadoop returned a non-zero exit code"
   echo "   128 - Move files Dropbox to Backup returned a non-zero exit code"
   echo "   129 - Deletion of files from Backup returned a non-zero exit code"
   echo "   130 - Folder consists of Zero Bytes Files"
   echo "   131 - Deletion of files returned a non-zero exit code"
   echo "   132 - Multiple DATA_AS_OF_DATE or DATA_AS_OF_DATE is not same as SYSTEM_DATE"
   echo "   133 - Backup Directory does not exists. "
   echo "   134 - Hive Table doesnot exists."
   echo "   135 - Issue while Kerberos Authentication ."
   echo "   136 - Input File Doesnot Exist ."
   echo "   137 - Orchestration process failed."
}   

function PARSE_INI(){
   # function inputs:
   # ${1} = fully qualified path to INI file
   # ${2} = name of the section

   eval `sed -e 's/[[:space:]]*\=[[:space:]]*/=/g' \
       -e 's/;.*$//' \
       -e 's/[[:space:]]*$//' \
       -e 's/^[[:space:]]*//' \
       -e "s/^\(.*\)=\([^\"']*\)$/\1=\"\2\"/" \
      < ${1} \
       | sed -n -e "/^\[${2}\]/,/^\s*\[/{/^[^;].*\=.*/p;}"`
}

function LOGGER(){
   # function inputs:
   # ${1} = message type (I, W, E)
   # ${2} = message to be logged
   #
   # The fields in the log message are dash (-) delimited, and are formatted as::
   #   field 1, srting(29) - timestamp in format:  YYYY-MM-DD HH:MI:SS.NNNNNNNNN 
   #   field 2, string(7), right padded with spaces - message type (INFO, WARNING, ERROR), 
   #   field 3, string(n) - descriptive message

   # default message type is Info

   MESSAGE_TYPE="INFO   "

   if [ "${1}" = "W" ]
   then
      MESSAGE_TYPE="WARNING"
   elif [ "${1}" = "E" ]
   then
     MESSAGE_TYPE="ERROR  "
   fi

   echo `date "+%Y-%m-%d %H:%M:%S.%N"`-"${MESSAGE_TYPE}"-"${2}" | tee -a ${LOG_FILE}
}



function FAILURE_NOTIFICATION()
{
    echo ""
    echo "---------------------------------------------------------------------------"
    echo ">>>>> ${INI_SECTION} failed during processing"
    echo "---------------------------------------------------------------------------"
    echo ""
    echo "Log file Name ${LOG_FILE}"
    echo "***************************************************************************"
    echo "**************************  END OF THE PROCESS  ***************************"
    echo "***************************************************************************"
    echo `date`
    echo ""
    `cat $LOG_FILE | \
    mailx -r $SENDER -s "Job failed on ${HOSTNAME} for ${INI_SECTION} " $NOTIFICATION_LIST`


}


function FAILURE(){
   # funtion inputs:
   # ${1} = the return code to exit with
   # Added for Orchestartion change -20190213
	CAPTURE_LOG=`echo "Exiting with custom return code ${1}"`
	ORCHESTRATION_STATUS FAILED
	if [ ${?} -ne 0 ]
	 then
   		LOGGER E "********** ERROR **********"
   		LOGGER E "Failed to execute JAVA API for orchestartion at FAILED stage"
	fi
    LOGGER E "Exiting with custom return code ${1}"
#Function call to insert data in STANDARD_DATA_SET_CONTEXT for Orchestration
LOAD_STANDARD_DATA_SET_CONTEXT ${TABLE_NAME} ${OWNER} 'FAILED' ${5}

### Uncomment below to enable failure notification
#	FAILURE_NOTIFICATION


#The ${JOB_NAME} Job has failed.  Please review the log file located at ${HOSTNAME}:${LOG_FILE} for more info.
#Here is the log generated during this run.

#Removes the Previous Version of Error in Error-Lock file
rm -rf ${BASEDIR}Error_Lock/COPY_${PROJECT}_${LOCK_FILE}_*
if [  ${?} -ne 0 ]
then
 LOGGER E "Error while removing Old lock files of Same File"
else
 LOGGER I "Old Lock Files of same file deleted successfully"
fi
ERROR_LOCK_FILE=${BASEDIR}Error_Lock/COPY_${PROJECT}_${LOCK_FILE}_${DATETIME}
#Creating New Lock file with latest timestamp in Error_Lock
mkdir -p ${ERROR_LOCK_FILE}
 if [ ${?} -ne 0 ]
        then
        LOGGER E "Error creating Lock file in Error_lock directory"
        else
        LOGGER I "Lock file in Error_lock directory created successfully"
 fi
#Remove the lock file from Lock Directory even if Job Fails
  rm -f ${COPY_LOCK_FILE}
if [  ${?} -ne 0 ]
then
 LOGGER E "Error while removing lock file from lock directory"
else
 LOGGER I "Lock File from Lock directory removed successfully"
fi

   exit 1
}

function ORCHESTRATION_STATUS(){
# function inputs
# ${1} = STATUS
STATUS=${1}
eval `sed -e 's/[[:space:]]*\=[[:space:]]*/=/g' \
       -e 's/;.*$//' \
       -e 's/[[:space:]]*$//' \
       -e 's/^[[:space:]]*//' \
       -e "s/^\(.*\)=\([^\"']*\)$/\1=\"\2\"/" \
      < ${VAR_DIR}/${API_VAR}`
if [ ${1} == 'RUNNING' ]
then
MESSAGE=`echo "Job execution in progress"`
if [[ ${IS_MANUAL} == 'Y' && -f ${VAR_DIR}/${API_VAR} ]]
then
LOGGER I "This is a manual file load and properties file also exists"
echo "jobName: ${AUTOSYS_JOB_NAME}"
echo "status: ${STATUS}"
echo "Message: ${MESSAGE}"
echo "loaddate: ${LOAD_DATE}"
echo "batchId: ${BATCH_ID}"
curl -k -X POST ${API_URL}\
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{"jobName":"'"${AUTOSYS_JOB_NAME}"'","status":"'"${STATUS}"'","message":"'"${MESSAGE}"'","loadDate":"'"${LOAD_DATE}"'","batchId":"'"${BATCH_ID}"'"} ';
elif [[ ${IS_MANUAL} == 'N' && -f ${VAR_DIR}/${API_VAR} && ${LOAD_DATE} == ${BUSINESS_DATE} ]]
then
LOGGER I "This is a auto file load, properties file also exists and load date matching with business date" 
echo "jobName: ${AUTOSYS_JOB_NAME}"
echo "status: ${STATUS}"
echo "Message: ${MESSAGE}"
echo "loaddate: ${LOAD_DATE}"
echo "batchId: ${BATCH_ID}"
curl -k -X POST ${API_URL}\
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{"jobName":"'"${AUTOSYS_JOB_NAME}"'","status":"'"${STATUS}"'","message":"'"${MESSAGE}"'","loadDate":"'"${LOAD_DATE}"'","batchId":"'"${BATCH_ID}"'"} ';
else
echo "jobName: ${AUTOSYS_JOB_NAME}"
echo "status: ${STATUS}"
echo "Message: ${MESSAGE}"
echo "loaddate: ${BUSINESS_DATE}"
curl -k -X POST ${API_URL}\
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{"jobName":"'"${AUTOSYS_JOB_NAME}"'","status":"'"${STATUS}"'","message":"'"${MESSAGE}"'","loadDate":"'"${BUSINESS_DATE}"'"} ';
fi

elif [ ${1} == 'FINISHED' ]
then
MESSAGE=`echo "Job executed successfully"`
echo "jobName: ${AUTOSYS_JOB_NAME}"
echo "status: ${STATUS}"
echo "Message: ${MESSAGE}"
echo "loaddate: ${LOAD_DATE}"
echo "batchId: ${BATCH_ID}"
curl -k -X POST ${API_URL}\
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{"jobName":"'"${AUTOSYS_JOB_NAME}"'","status":"'"${STATUS}"'","message":"'"${MESSAGE}"'","loadDate":"'"${LOAD_DATE}"'","batchId":"'"${BATCH_ID}"'"} ';
else
MESSAGE=`echo "Job execution failed: $CAPTURE_LOG"`
echo "jobName: ${AUTOSYS_JOB_NAME}"
echo "status: ${STATUS}"
echo "Message: ${MESSAGE}"
echo "loaddate: ${LOAD_DATE}"
echo "batchId: ${BATCH_ID}"
curl -k -X POST ${API_URL}\
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{"jobName":"'"${AUTOSYS_JOB_NAME}"'","status":"'"${STATUS}"'","message":"'"${MESSAGE}"'","loadDate":"'"${LOAD_DATE}"'","batchId":"'"${BATCH_ID}"'"} ';
fi
}


function CHECK_NULL_VAR(){
   # function inputs:
   # ${1} = variable name
   # ${2} = variable value
   # ${3} = exit code to use when check fails
   #        if exit code is null, then do not fail

   LOGGER I "Begin - confirm ${1} has been set and has a value"
   if [ "X${2}" = "X" ]
   then
      if [ "X${3}" = "X" ]
      then
         LOGGER I "Value for ${1} is null (not an error condition)"
      else
         LOGGER E "********** ERROR **********"
         LOGGER E "A valid value for ${1} has not been set"
         FAILURE ${3};
      fi
   fi
   LOGGER I "${1} has been set to: ${2}"
   LOGGER I "End - confirm ${1} has been set and has a value"
}

function CHECK_FILE_WRITABLE(){
   # function inputs
   # ${1} = variable name
   # ${2} = variable value - fully qualified file name
   # ${3} = exit code to use when check fails
   #        if exit code is null, then do not fail

   # need a variable so we know if we need to RM our test file or not
   TEMP_TOUCH_FLAG=0

   LOGGER I "Begin - Check if ${1} file ${2} is writable"
   # first we have to check if the exists or not
   if ! [ -f ${2} ]
   then
      LOGGER I "Touching a temp file for writable validation"
      TEMP_TOUCH_FLAG=1
      touch ${2} > /dev/null 2>&1
   fi

   if ! [ -w ${2} ]
   then
      if [ "X${3}" = "X" ]
      then
         LOGGER W "${1} file ${2} is not writable (not an error condition)"
      else
         LOGGER E "********* ERROR **********"
         LOGGER E "${1} file ${2} is not wiritable"
         LOGGER E "Check the location and permissions"
         FAILURE ${3}
      fi
   fi

   if [ ${TEMP_TOUCH_FLAG} -ne 0 ]
   then
      LOGGER I "Removing the temp file used for writable validation"
      rm -f ${2} > /dev/null 2>&1
   fi

   LOGGER I "End - check if ${1} file ${2} is writable"
}

function CHECK_FILE_EXISTS(){
   # function inputs
   # ${1} = variable name
   # ${2} = varibale value - fully qualified file name
   # ${3} = exit code to use when check fails
   #        if exit code is null, then do not fail

   LOGGER I "Begin - Check if ${1} file ${2} exists"
   if ! [ -f ${2} ]
   then
      if [ "X${3}" = "X" ]
      then
         LOGGER W "${1} file ${2} does not exist (not an error condition)"
      else
         LOGGER E "********* ERROR **********"
         LOGGER E "${1} file ${2} does not exist"
         LOGGER E "Research cause of missing file, create or restore file if needed"
         FAILURE ${3}
      fi
   else
      LOGGER I "${1} file ${2} exists"
   fi
   LOGGER I "End - check if ${1} file ${2} exists"
}

function CHECK_DIRECTORY_EXISTS(){
   # function inputs
   # ${1} = variable name
   # ${2} = varibale value - fully qualified directory name
   # ${3} = exit code to use when check fails
   #        if exit code is null, then do not fail

   LOGGER I "Begin - Check if ${1} directory ${2} exists"
   if ! [ -d ${2} ]
   then
      if [ "X${3}" = "X" ]
      then
         LOGGER W "${1} directory ${2} does not exist (not an error condition)"
      else
         LOGGER E "********* ERROR **********"
         LOGGER E "${1} directory ${2} does not exist"
         LOGGER E "Research cause of missing directory, create or restore directory if needed"
         FAILURE ${3}
      fi
   else
      LOGGER I "${1} directory ${2} exists"
   fi
   LOGGER I "End - check if ${1} directory ${2} exists"
}

function REMOVE_HDFS_FILES(){
# function inputs
   # ${1} = variable name
   # ${2} = Directory Path


     LOGGER I "Begin - Removing Files from Hadoop Directory ${2}"
     hadoop fs -rm -skipTrash ${2}*
     if [ ${?} -ne 0 ]
        then
        LOGGER E "Error Removing Files from ${2} or No Such Files"
        else
        LOGGER I "Files removed successfully from ${2}"
        LOGGER I "End - Removing Files from Hadoop Directory ${2}"
     fi
}


function CHECK_HIVE_TABLE_EXIST(){
# function inputs
   # ${1} = variable name
   # ${2} = Database name
   # ${3} = varibale value - table name
   # ${4} = exit code to use when check fails
   #        if exit code is null, then do not fail
CHECK_HIVE_TABLE_FLAG=0
   LOGGER I "Begin - Check if ${1} ${3} exists"
   hive -e  " select * from ${2}.${3} limit 1;"  -v 1>>${LOG_FILE}.tmp 2>&1
 HIVE_RET_CODE=${?}
   # this .tmp log is to ensure that we capture all of the hive output to the log
   LOGGER I "`cat ${LOG_FILE}.tmp`i"
   rm -f ${LOG_FILE}.tmp
   if [ ${HIVE_RET_CODE} -ne 0 ]
   then
        echo "Table does not exist"
        CHECK_HIVE_TABLE_FLAG=0
        LOGGER E "${1} table ${3} does not exist"
        FAILURE 134
   else
        echo "Table exists"
        CHECK_HIVE_TABLE_FLAG=1
   fi
        LOGGER I "End - Check if ${1} table ${3} exists"
}

function EXECUTE_SPARK_SQL_DML(){
# function inputs
   # ${1} = table name
   # ${2} = database name
   # 3 = hsql path
   # 4 = PHASE
LOGGER I "BEGIN - Executing SPARK_SQL DML ${3}"
if [ "${4}" == "AS_IS"  ]
then
	
	#Replacing Business Date in HQL as we need to put data for particular Day
        PARAMETERISED_HQL=${2}_${DATETIME}
	#sed   -e 's/$BUSINESS_DATE/'${BUSINESS_DATE}'/g'  ${2} > ${PARAMETERISED_HQL}
sed   -e 's/$DATA_AS_OF_DAILY_DATE/'${DATA_AS_OF_DAILY_DATE}'/g;s/$DATA_AS_OF_DAILY_DATE_MINUS_1/'${DATA_AS_OF_DAILY_DATE_MINUS_1}'/g;s/$DATA_AS_OF_DAILY_DATE_MINUS_2/'${DATA_AS_OF_DAILY_DATE_MINUS_2}'/g;s/$DATA_AS_OF_YEAR_MONTH/'${DATA_AS_OF_YEAR_MONTH}'/g;s/$DATA_AS_OF_QUARTER/'${DATA_AS_OF_QUARTER}'/g;s/$DATA_AS_OF_YEAR_FIRST_DAY/'${DATA_AS_OF_YEAR_FIRST_DAY}'/g;s/$DATA_AS_OF_YEAR_FIRST_DAY_MINUS_1/'${DATA_AS_OF_YEAR_FIRST_DAY_MINUS_1}'/g;s/$DATA_AS_OF_YEAR/'${DATA_AS_OF_YEAR}'/g' ${2} > ${PARAMETERISED_HQL}	

	#spark2-submit \
spark2-submit  \
--master yarn \
--deploy-mode cluster \
--conf 'spark.executor.extraJavaOptions=-XX:NewRatio=1 -XX:+UseParallelGC -XX:SurvivorRatio=1 -XX:ParallelGCThreads=4 ' \
--files "${3},/etc/hive/conf/hive-site.xml" \
--class org.ubs.mrp.SubmitSparkSQL \
/root/MRPPOC/INGESTION/code/ingestion_java-1.0-SNAPSHOT.jar \
"${1}" "${2}" "${4}" "${1}.hql"

    SPARK_RET_CODE=${?}
	if [ ${SPARK_RET_CODE} -ne 0 ]
    then
		LOGGER E "********** ERROR **********"
        LOGGER E "Spark exited with a non-zero exit code: ${SPARK_RET_CODE}"
		
		#rm -f ${PARAMETERISED_HQL}
		#LOGGER I "BEGIN - Remove files from Partition directory"
        #REMOVE_HDFS_FILES PartionDirectoryFiles  ${PARTITION_DIRECTORY}
		#LOGGER I "END - Remove files from Partition directory"	
		FAILURE 126
	fi
fi
	#Removing Parameterized Temporary Query
	#rm -f ${PARAMETERISED_HQL}
	LOGGER I "END - Executing DML ${2}"
}

function EXECUTE_DML(){
# function inputs
   # ${1} = variable name
   # ${2} = hsql path
LOGGER I "BEGIN - Executing DML ${2}"
#For Governed we need to run HQL in Impala for one time and if failure occur, retry it in Hive

#Replacing Business Date in HQL as we need to put data for particular Day
        PARAMETERISED_HQL=${2}_${DATETIME}
#PARAMETERISED_HQL=${2}
        #sed   -e 's/$BUSINESS_DATE/'${BUSINESS_DATE}'/g'  ${2} > ${PARAMETERISED_HQL}

sed   -e 's/$COB_DATE_PLUS_1_STR/'${COB_DATE_PLUS_1_STR}'/g;s/$CONTEXT_ID/'${CONTEXT_ID}'/g;s/$MCR_FILENAME/'${MCR_FILENAME}'/g;s/$FILE_DATE/'${FILE_DATE}'/g;s/$BUSINESS_DAY/'${BUSINESS_DAY}'/g;s/$DATA_AS_OF_LAST_DAY_OF_YEAR_MINUS_7/'${DATA_AS_OF_LAST_DAY_OF_YEAR_MINUS_7}'/g;s/$DATA_AS_OF_LAST_DAY_PREVIOUS_YEAR/'${DATA_AS_OF_LAST_DAY_PREVIOUS_YEAR}'/g;s/$DATA_AS_OF_YEAR_FIRST_DAY_MINUS_1/'${DATA_AS_OF_YEAR_FIRST_DAY_MINUS_1}'/g;s/$DATA_AS_OF_LAST_DAY_OF_MONTH/'${DATA_AS_OF_LAST_DAY_OF_MONTH}'/g;s/$DATA_AS_OF_LAST_DAY_OF_YEAR/'${DATA_AS_OF_LAST_DAY_OF_YEAR}'/g;s/$DATA_AS_OF_YEAR_MINUS_1/'${DATA_AS_OF_YEAR_MINUS_1}'/g;s/$DATA_AS_OF_DAILY_DATE_MINUS_1/'${DATA_AS_OF_DAILY_DATE_MINUS_1}'/g;s/$DATA_AS_OF_DAILY_DATE_MINUS_2/'${DATA_AS_OF_DAILY_DATE_MINUS_2}'/g;s/$DATA_AS_OF_DAILY_DATE_MINUS_3/'${DATA_AS_OF_DAILY_DATE_MINUS_3}'/g;s/$DATA_AS_OF_DAILY_DATE/'${DATA_AS_OF_DAILY_DATE}'/g;s/$DATA_AS_OF_YEAR_MONTH/'${DATA_AS_OF_YEAR_MONTH}'/g;s/$DATA_AS_OF_QUARTER/'${DATA_AS_OF_QUARTER}'/g;s/$DATA_AS_OF_YEAR_FIRST_DAY/'${DATA_AS_OF_YEAR_FIRST_DAY}'/g;s/$DATA_AS_OF_YEAR/'${DATA_AS_OF_YEAR}'/g;s/$DATA_AS_OF_LAST_DAY_OF_LAST_MONTH/'${DATA_AS_OF_LAST_DAY_OF_LAST_MONTH}'/g' ${2} > ${PARAMETERISED_HQL}

PARAM_SQL=$(cat ${PARAMETERISED_HQL})

LOGGER I "PARAMETERIZED_HQL:::${PARAMETERISED_HQL}"
LOGGER I "PARAMETERIZED_HQL:::: $PARAM_SQL"


if [ "${3}" == "Governed"  ] || [ "${3}" == "Etl"  ] || [ "${3}" == "Provision"  ]
then
	LOGGER I "Exectuting via Impala"
	#impala-shell --ssl -k --ca_cert $KERBEROS_CERT --config "${BASEDIR}config/impala-config" -f ${PARAMETERISED_HQL} >> ${LOG_FILE}
	impala-shell --config "${BASEDIR}config/impala-config" -f ${PARAMETERISED_HQL} >> ${LOG_FILE}
      	IMPALA_RET_CODE=${?}
	 if [ ${IMPALA_RET_CODE} -ne 0 ]
        then
                LOGGER E "********** ERROR **********"
                LOGGER E "Impala exited with a non-zero exit code: ${IMPALA_RET_CODE}"
				UPDATE_CONTEXT FAILED
		rm -f ${PARAMETERISED_HQL}
        	LOGGER I "BEGIN - Remove files from Partition directory"
                REMOVE_HDFS_FILES PartionDirectoryFiles  ${PARTITION_DIRECTORY}
          	LOGGER I "END - Remove files from Partition directory"	
		LOGGER I "BEGIN - Refresh Data"
		REFRESH_QUERY="REFRESH \`$DATABASE_NAME\`.\`$TABLE_NAME\`"
		impala-shell --ssl -k --ca_cert $KERBEROS_CERT --config "${BASEDIR}config/impala-config" -q "$REFRESH_QUERY" 
		 if [ ${?} -ne 0 ]
                 then
                        LOGGER E "********** ERROR **********"
                        LOGGER E "Refresh Table exited with a non-zero exit code"
                        FAILURE 126
                 fi
                LOGGER I "END - Refresh Data"
                FAILURE 126

	fi
	LOGGER I "BEGIN - Incremental Compute Stats Table"	
	COMPUTE_QUERY="COMPUTE INCREMENTAL STATS \`$DATABASE_NAME\`.\`$TABLE_NAME\`"	
	impala-shell --config "${BASEDIR}config/impala-config" -q "$COMPUTE_QUERY"
	 if [ ${?} -ne 0 ]
                then
                LOGGER E "********** ERROR **********"
                LOGGER E "Compute Incremental Stats Table exited with a non-zero exit code"
                FAILURE 126
                
	else
	UPDATE_CONTEXT SUCCESS
	LOGGER I "END - Incremental Compute Stats Table"	
	fi
	



#For AS_IS Base we need to run HQL in Hive only
elif [ "${3}" == "Base" ]
then

	if [ "${IS_MULTICHAR_DELIM}" == "N" ]
	then
		LOGGER I "Executing ${2} via Impala"
		impala-shell --config "${BASEDIR}config/impala-config" -f ${PARAMETERISED_HQL} >> ${LOG_FILE}
		RET_CODE=${?}
	else
		LOGGER I "Executing ${2} via Hive"
		hive -f ${PARAMETERISED_HQL} >> ${LOG_FILE}
		RET_CODE=${?}
	fi

        #hive -f ${2} >> ${LOG_FILE}

	#RET_CODE=${?}
	if [ ${RET_CODE} -ne 0 ]
	then
        	LOGGER E "********** ERROR **********"
        	LOGGER E "HIVE or Impala exited with a non-zero exit code: ${RET_CODE}"
        	LOGGER E "Research the error, and correct the condition before restarting"
        	LOGGER E "Be confident of any data which may have already processed prior"
        	LOGGER E "to the error (be careful not to rerun incorrectly"
        	LOGGER E "and potentially duplicating data)"
			UPDATE_CONTEXT FAILED
        	FAILURE 126
	fi
	LOGGER I "BEGIN - Refresh Data"
	REFRESH_QUERY="REFRESH \`$DATABASE_NAME\`.\`$TABLE_NAME\`"
	impala-shell --config "${BASEDIR}config/impala-config" -q "$REFRESH_QUERY"
	if [ ${?} -ne 0 ]
	then
                LOGGER E "********** ERROR **********"
                LOGGER E "Refresh Table exited with a non-zero exit code"
                FAILURE 126
	else 
		UPDATE_CONTEXT SUCCESS
	fi
	LOGGER I "END - Refresh Data"
fi

#Removing Parameterized Temporary Query
rm -f ${PARAMETERISED_HQL}
LOGGER I "END - Executing DML ${2}"
 
}



function UPDATE_CONTEXT(){
# function inputs
	# ${1} = Success/Failure
	

	if [ ${1} == 'SUCCESS' ]  && [ ${OWNER} != 'ETL' ]
	then
		impala-shell --config "${BASEDIR}config/impala-config" -q " INSERT OVERWRITE ETL.BATCH_DATE_STATUS SELECT '${DATA_AS_OF_DAILY_DATE}' AS BATCH_DATE, '${TABLE_NAME}' AS TABLE_NAME, '${OWNER}' AS LAYER, 'COMPLETED' AS STATUS "
	CONTROL_FILE_ARRAY=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "SELECT DISTINCT CONTROL_FILE FROM ETL.CONTROL_FILE_MAPPING WHERE ((AS_IS_TABLE='${TABLE_NAME}' AND '${OWNER}'='AS_IS') OR (GOVERNED_TABLE='${TABLE_NAME}' AND '${OWNER}'='GOVERNED') OR (PROVISION_TABLE='${TABLE_NAME}' AND '${OWNER}'='PROVISION'))")
LOGGER I "${CONTROL_FILE_ARRAY[0]}- control file for AS_IS"	
	### Loop through each control file and call update context for each.
	for CONTROL_FILE in "${CONTROL_FILE_ARRAY[@]}"
	do 
		LOGGER I "**********Begin Update Procedure for ${CONTROL_FILE}**********"
		LOGGER I "Owner: $OWNER :: Database: $DATABASE_NAME"
		EXECUTE_UPDATE_CONTEXT_PROCEDURE
		if [ ${?} -ne 0 ]
		then
		FAILURE 198
		fi
	done
	
	fi

}



function EXECUTE_UPDATE_CONTEXT_PROCEDURE() {
# function Inputs
	# ${1} = Oracle User ID
	# ${2} = Oracle Password
	# ${3} = Oracle Connection String
	# ${3} = Control File Name
	# ${3} = Context ID
	# ${3} = Success Flag
	# ${3} = Database Layer
#. /home/edsappproc/EDS/.Variables

USERNAME=$EDS_ORCL_USER
PASSWORD=$EDS_ORCL_PASS
ts=`date '+%Y%m%d%H%M%S'`
LOGGER I "Within EXECUTE_UPDATE_CONTEXT_PROCEDURE"
LOGGER I  "${CONTROL_FILE_DIR} -control file directory"
LOGGER I "${CONTROL_FILE} -control file"
if [ -e ${CONTROL_FILE_DIR}/$CONTROL_FILE ]; then
LOGGER I "${CONTROL_FILE_DIR}/$CONTROL_FILE is present"
fi
if [ "${OWNER}" = "AS_IS" ] && [ -f ${CONTROL_FILE_DIR}/$CONTROL_FILE ]
then
LOGGER I "Owner: $OWNER :: Database: $DATABASE_NAME"
FEED_CONTEXT_ID=`awk '{print $NF}' ${CONTROL_FILE_DIR}/$CONTROL_FILE`
LOGGER I "FEED_CONTEXT_ID for current iteration, for ASIS: ${FEED_CONTEXT_ID}"

sqlplus ${USERNAME}/${PASSWORD}@${DATABASE_CONNECTION} <<!
set serveroutput on;
DECLARE
   P_STATUS      VARCHAR2 (10);
   P_ERROR_MSG   VARCHAR2 (10);
BEGIN
   manage_context.update_context (${FEED_CONTEXT_ID},
                                  NULL,
                                  NULL,
                                  NULL,
                                  NULL,
                                  NULL,
                                  'Y',
                                  NULL,
                                  NULL,
                                  P_STATUS,
                                  P_ERROR_MSG);
END;
/

update FEED_CONTEXT set MANUAL_FILE_UPLOAD_STATUS='COMPLETED' where FEED_CONTEXT_ID=${FEED_CONTEXT_ID};
commit;
set serveroutput off;
exit
!

##### 	Move Control file to Archive if ASIS Load is complete. Use Archived File for Governed & Provision.
mv ${CONTROL_FILE_DIR}/$CONTROL_FILE ${CONTROL_FILE_ARCHIVE_DIR}/$CONTROL_FILE.${DATA_AS_OF_DAILY_DATE}
LOGGER I "Moving control file- from ${CONTROL_FILE_DIR}/$CONTROL_FILE to ${CONTROL_FILE_ARCHIVE_DIR}/$CONTROL_FILE.${DATA_AS_OF_DAILY_DATE}"

elif [ "${OWNER}" = "GOVERNED" ] && [ -f ${CONTROL_FILE_ARCHIVE_DIR}/$CONTROL_FILE.${DATA_AS_OF_DAILY_DATE} ]
then
FEED_CONTEXT_ID=`awk '{print $NF}' ${CONTROL_FILE_ARCHIVE_DIR}/$CONTROL_FILE.${DATA_AS_OF_DAILY_DATE}`
LOGGER I "FEED_CONTEXT_ID for current iteration, for Governed: ${FEED_CONTEXT_ID}"


sqlplus ${USERNAME}/${PASSWORD}@${DATABASE_CONNECTION} <<!
set serveroutput on;
DECLARE
   P_STATUS      VARCHAR2 (10);
   P_ERROR_MSG   VARCHAR2 (10);
BEGIN
   manage_context.update_context (${FEED_CONTEXT_ID},
                                  NULL,
                                  NULL,
                                  NULL,
                                  NULL,
                                  NULL,
                                  'Y',
                                  'Y',
                                  NULL,
                                  P_STATUS,
                                  P_ERROR_MSG);
END;
/
commit;
set serveroutput off;
exit
!


elif [ "${OWNER}" = "PROVISION" ] && [ -f ${CONTROL_FILE_ARCHIVE_DIR}/$CONTROL_FILE.${DATA_AS_OF_DAILY_DATE} ]
then
FEED_CONTEXT_ID=`awk '{print $NF}' ${CONTROL_FILE_ARCHIVE_DIR}/$CONTROL_FILE.${DATA_AS_OF_DAILY_DATE}`
LOGGER I "FEED_CONTEXT_ID for current iteration, for Provision: ${FEED_CONTEXT_ID}"


sqlplus ${USERNAME}/${PASSWORD}@${DATABASE_CONNECTION} <<!
set serveroutput on;
DECLARE
   P_STATUS      VARCHAR2 (10);
   P_ERROR_MSG   VARCHAR2 (10);
BEGIN
   manage_context.update_context (${FEED_CONTEXT_ID},
                                  NULL,
                                  NULL,
                                  NULL,
                                  NULL,
                                  NULL,
                                  'Y',
                                  'Y',
                                  'Y',
                                  P_STATUS,
                                  P_ERROR_MSG);
END;
/
commit;
set serveroutput off;
exit
!


fi          
}

function CHECK_CONTROL_FILE_COUNT_WITH_ACTUAL() {

CONTROL_FILE_RECORD_COUNT=`cat ${CONTROL_FILE_DIR}/$CONTROL_FILE | awk '{ print $3}'`
ACTUAL_FILE_RECORD_COUNT=`cat ${DROPBOX_PATH}/${INPUT_DIR}$FILENAME | wc -l`
if [ ${CONTROL_FILE_RECORD_COUNT} -eq ${ACTUAL_FILE_RECORD_COUNT} ]
then
        LOGGER I "Counts between Control File and Source File match "
else
        LOGGER E "********** ERROR **********"
        LOGGER E "Count Mismatch for Control file and Source File"
        FAILURE 137
fi

}

function SOURCE_FILE_MERGE(){
#function inputs
# ${1} = full input file path
# ${2} = input directory path
# ${3} = input file name
# ${4} = Table name
# ${5} = error code in case input file 

LOGGER I "Inside SOURCE_FILE_MERGE function"
LOGGER I "Full input file path : ${1} Full input directory path : ${2} Full input file name : ${3}"


#check if multiple input files are present

file_count="$(find "${2}" -maxdepth 1 -name "${3}" | wc -l)" #update global variable for file_count

if [ $file_count -eq 0 ]
then
LOGGER E "********* ERROR **********"
LOGGER E "INPUT_FILE ${1} does not exist"
LOGGER E "Research cause of missing file, create or restore file if needed"
FAILURE ${5}
fi

LOGGER I "Input File Count = ${file_count}"


# merge multiple input files into single input file

if [ $file_count -gt 1 ]
then 
LOGGER I "Multiple input files present"
#readarray file_name < <(find "${2}" -maxdepth 1 -name "${3}")
temp_file=Temp_${3}_files.txt
LOGGER I "Creating a temp file to store multiple input file names: ${temp_file}"
touch ${temp_file}
find "${2}" -maxdepth 1 -name "${3}">${temp_file} #save multiple file names to a temp file
i=0
while IFS=$'\n' read -r line 
do
LOGGER I "$line"
file_name[i]="$line" #write MCR multiple file names into global array. 
i=`expr "$i" + 1`
LOGGER I "${i}"
done <$temp_file 
LOGGER I "File names stored in FILE_NAME array. Removing temp file: ${temp_file}"
rm ${temp_file}
file_count_new=`expr $file_count - 1` 
new_file_temp=`basename $(ls $1 |head -1| sed 's/*.*//')` #extract name for new consolidated input file
LOGGER I "Extracted name: ${new_file_temp}"
touch ${2}/${new_file_temp}.txt #new consolidated input file
file_new=${2}/${new_file_temp}.txt
file_consolidated=`basename ${file_new}` #global variable updated
LOGGER I "The consolidated input file name is: ${file_consolidated}"
X=0
LOGGER I "File name in array at $X position- ${file_name[$X]}"
TEST_VAR=${file_name[$X]}
LOGGER I " TEST_VAR= ${TEST_VAR} "
for (( X=0; X<=${file_count_new}; X++ )) 
do
LOGGER I "X= $X"
# Remove Header from each source file
curr_file=${file_name[$X]}
LOGGER I "CURR_FILE=${curr_file}"
name_prefix="${4:0:7}" #store table name prefix to check how many headers would the source file have

LOGGER I "Table Name prefix- ${name_prefix}"

if [ "${name_prefix}" == "GCRS_FX" -o "${name_prefix}" == "MCR_FX_" ]
then
LOGGER I "Source files contain three headers"
sed '1,3d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}

elif [ "${name_prefix}" == "MCR_GPM" ] || [ "${name_prefix}" == "MCR_PNL" ] || [ "${name_prefix}" == "MCR_CLI" ] || [ "${name_prefix}" == "MCR_HEA" ] || [ "${name_prefix}" == "MCR_STA" ] || [ "${name_prefix}" == "MCR_BAL" ]
then
LOGGER I "Source files contain two headers"
sed '1,2d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}

else
LOGGER I "Source files contain one header"
sed '1d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}

fi

if [ $X -eq 0 ]
then
cat ${curr_file}>${file_new} #first time overwrite

else
echo "">>${file_new}
cat ${curr_file}>>${file_new} #other times append

fi

done
impala-shell --config "${BASEDIR}config/impala-config" -q "INSERT OVERWRITE ETL.WORK_FILE_NAME_MAPPING PARTITION (TABLE_NAME,DATA_AS_OF_DAILY_DATE) SELECT '${file_consolidated}' AS FILE_NAME,'${TABLE_NAME}' AS TABLE_NAME,'${DATA_AS_OF_DAILY_DATE}' AS DATA_AS_OF_DAILY_DATE"
impala-shell --config "${BASEDIR}config/impala-config" -q "COMPUTE INCREMENTAL STATS ETL.WORK_FILE_NAME_MAPPING"

else
LOGGER I "Single MCR source file present"
curr_file="$(find "${2}" -maxdepth 1 -name "${3}")"
LOGGER I "Single MCR full source file path is: ${curr_file}"
file_consolidated=`basename ${curr_file}` #update global variable to single file file name
LOGGER I "Single MCR source file name is: ${file_consolidated}"
impala-shell --config "${BASEDIR}config/impala-config" -q "INSERT OVERWRITE ETL.WORK_FILE_NAME_MAPPING PARTITION (TABLE_NAME,DATA_AS_OF_DAILY_DATE) SELECT '${file_consolidated}' AS FILE_NAME,'${TABLE_NAME}' AS TABLE_NAME,'${DATA_AS_OF_DAILY_DATE}' AS DATA_AS_OF_DAILY_DATE"
impala-shell --config "${BASEDIR}config/impala-config" -q "COMPUTE INCREMENTAL STATS ETL.WORK_FILE_NAME_MAPPING"
#Remove header from single source file
name_prefix="${4:0:7}" #store table name prefix to check how many headers would the source file have

LOGGER I "Table Name prefix- ${name_prefix}"
if [ "${name_prefix}" == "GCRS_FX" -o "${name_prefix}" == "MCR_FX_" ]
then
LOGGER I "Source file contains three headers"
sed '1,3d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}

elif [ "${name_prefix}" == "MCR_GPM" ] || [ "${name_prefix}" == "MCR_PNL" ] || [ "${name_prefix}" == "MCR_CLI" ]  || [ "${name_prefix}" == "MCR_HEA" ] || [ "${name_prefix}" == "MCR_STA" ] || [ "${name_prefix}" == "MCR_BAL" ]
then
LOGGER I "Source file contains two headers"
sed '1,2d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}

else
LOGGER I "Source file contains one header"
sed '1d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}

fi

fi

curr_file="$(find "${2}" -maxdepth 1 -name "${3}")"
trail=`tail -1 ${curr_file} | grep -o 'TRAILER'`
LOGGER I "BEGIN :Remove the trailer from the source file"
if  [ "$trail" = "" ]
then
LOGGER I "No trailer present in the file"
else
sed '$d' ${curr_file} > ${curr_file}_tmp; mv -f ${curr_file}_tmp ${curr_file}
LOGGER I "Removed trailer"
fi

}

function SOURCE_FILE_MERGE_RWA(){
#This merges Part1 RWA files by removing 26 lines header & merges Part2 files by removing 20 lines header
#function inputs
# ${1} = full input file path
# ${2} = input directory path
# ${3} = input file name
# ${4} = Table name
# ${5} = error code in case input file

LOGGER I "Inside SOURCE_FILE_MERGE_RWA function"
LOGGER I "Full input file path : ${1} Full input directory path : ${2} Full input file name : ${3}"

cd ${2}
echo "File name is $FILENAME"

if [ ! -f $FILENAME ]
then
echo "File $FILENAME does not exist"
LOGGER E "File $FILENAME not present "
fi

FILE_NAME_1=`ls $FILENAME`
#FILE_NAME_2=`ls FRS_A_RW51*`
num=0
#cat ${FILE_NAME_1}|head -2|tail -1 >${TABLE_NAME}.txt
touch ${TABLE_NAME}.txt
#cat FRS_A_RW51*|head -2|tail -1 >FRS_ACT_RWA_PART2
FILE_PATTERN=`echo ${FILENAME}|cut -c 1-8`
TABLE_PATTERN=`echo ${TABLE_NAME}|awk -F '_' '{print $6}'`
temp_file=Temp_${FILE_NAME_1}
ls ${FILE_PATTERN}*|grep -v i>$temp_file
while IFS=$'\n' read -r rwa_list
#for rwa_list in $(ls -ltr ${FILE_PATTERN}*|awk -F' ' '{print $9;}'|grep -v i)
do 
num=`echo ${rwa_list}|awk -F '_' '{print $3;}'|cut -c 3-`
echo $num
LOGGER I "This file has number $num"
if [ $num -le 50 ] && [ "$TABLE_PATTERN" == "PART1" ]
then 
echo ${rwa_list}
sed -e '1,26d' ${rwa_list} >>${TABLE_NAME}.txt
LOGGER I "Merging RW1 files by removing header" 
mv ${rwa_list} $DROPBOX_ARCHIVED_PATH
mv ${rwa_list}i* $DROPBOX_ARCHIVED_PATH
elif [ $num -gt 50 ] && [ "$TABLE_PATTERN" == "PART2" ]
then
sed -e '1,20d' ${rwa_list} >>${TABLE_NAME}.txt
LOGGER I "Merging RW2 files by removing header"
mv ${rwa_list} $DROPBOX_ARCHIVED_PATH
mv ${rwa_list}i* $DROPBOX_ARCHIVED_PATH
fi
done<$temp_file
mv $TABLE_NAME.txt $FILE_NAME_1
rm $temp_file
FILENAME=$FILE_NAME_1

impala-shell --config "${BASEDIR}config/impala-config" -q "INSERT OVERWRITE ETL.WORK_FILE_NAME_MAPPING PARTITION (TABLE_NAME,DATA_AS_OF_DAILY_DATE) SELECT '${FILENAME}' AS FILE_NAME,'${TABLE_NAME}' AS TABLE_NAME,'${DATA_AS_OF_DAILY_DATE}' AS DATA_AS_OF_DAILY_DATE"
impala-shell --config "${BASEDIR}config/impala-config" -q "COMPUTE INCREMENTAL STATS ETL.WORK_FILE_NAME_MAPPING"
}

function SOURCE_FILE_MERGE_LRD(){
#This merges LRD Part 1, Part 2 & Part 3 files by removing 2 lines header 
#function inputs
# ${1} = full input file path
# ${2} = input directory path
# ${3} = input file name
# ${4} = Table name
# ${5} = error code in case input file

LOGGER I "Inside SOURCE_FILE_MERGE_LRD function"
LOGGER I "Full input file path : ${1} Full input directory path : ${2} Full input file name : ${3}"

cd ${2}
echo "File name is $FILENAME"

if [ ! -f $FILENAME ]
then
echo "File $FILENAME does not exist"
LOGGER E "File $FILENAME not present "
fi

FILE_NAME_1=`ls $FILENAME`
num=0
#cat ${FILE_NAME_1}|head -2|tail -1 >${TABLE_NAME}.txt
touch ${TABLE_NAME}.txt
FILE_PATTERN=`echo ${FILENAME}|cut -c 1-8`
TABLE_PATTERN=`echo ${TABLE_NAME}|awk -F '_' '{print $6}'`
temp_file=Temp_${FILE_NAME_1}
ls ${FILE_PATTERN}*|grep -v i>$temp_file
while IFS=$'\n' read -r lrd_list
do
num=`echo ${lrd_list}|awk -F '_' '{print $3;}'|cut -c 3-`
echo $num
LOGGER I "This file has number $num"
if [ $num -le 50 ] && [ "$TABLE_PATTERN" == "GROUP" ]
then
echo ${lrd_list}
sed -e '1,2d' ${lrd_list} >>${TABLE_NAME}.txt
LOGGER I "Merging LRD Part1 files by removing header"
mv ${lrd_list} $DROPBOX_ARCHIVED_PATH
mv ${lrd_list}i* $DROPBOX_ARCHIVED_PATH
elif [ $num -gt 50 ] && [ $num -le 90 ] && [ "$TABLE_PATTERN" == "STANDALONE" ]
then
sed -e '1,2d' ${lrd_list} >>${TABLE_NAME}.txt
LOGGER I "Merging LRD Part 3 files by removing header"
mv ${lrd_list} $DROPBOX_ARCHIVED_PATH
mv ${lrd_list}i* $DROPBOX_ARCHIVED_PATH
elif [ $num -gt 90 ] && [ "$TABLE_PATTERN" == "DEDUCTIONS" ]
then
sed -e '1,2d' ${lrd_list} >>${TABLE_NAME}.txt
LOGGER I "Merging LRD Part 3 files by removing header"
mv ${lrd_list} $DROPBOX_ARCHIVED_PATH
mv ${lrd_list}i* $DROPBOX_ARCHIVED_PATH
fi
done<$temp_file
mv $TABLE_NAME.txt $FILE_NAME_1
rm $temp_file
FILENAME=$FILE_NAME_1

impala-shell --config "${BASEDIR}config/impala-config" -q "INSERT OVERWRITE ETL.WORK_FILE_NAME_MAPPING PARTITION (TABLE_NAME,DATA_AS_OF_DAILY_DATE) SELECT '${FILENAME}' AS FILE_NAME,'${TABLE_NAME}' AS TABLE_NAME,'${DATA_AS_OF_DAILY_DATE}' AS DATA_AS_OF_DAILY_DATE"
impala-shell --config "${BASEDIR}config/impala-config" -q "COMPUTE INCREMENTAL STATS ETL.WORK_FILE_NAME_MAPPING"
}


function OVERWRITE_FILE_FROM_DROPBOX_TO_HADOOP(){
# function inputs
   # ${1} = variable name
   # ${2} = varibale value - file name
   # ${3} = value - fully classified hadoopPath
   # ${4} = exit code to use when check fails
   #        if exit code is null, then do not fail
LOGGER I "Inside OVERWRITE_FILE_FROM_DROPBOX_TO_HADOOP function"

#Check if source is MCR
if [ "${SOURCE}" != "MCR" ]
then
#Remove header from non MCR source file

LOGGER I "Begin - Remove Header from non MCR source file"

sed '1d' ${2} > ${2}_tmp; mv -f ${2}_tmp ${2}

LOGGER I "End - Removed Header from non MCR source file"

trail=`tail -1 ${2} | grep 'RecordCount\|Trailer'`
LOGGER I "BEGIN :Remove the trailer from the source file"
if  [ $trail != "" ]
then
 sed '$d' ${2} > ${2}_tmp; mv -f ${2}_tmp ${2}
  LOGGER I "Removed trailer"
else
    LOGGER I "No trailer present in the file"
fi


fi
  LOGGER I "Begin - Copy ${1} file ${2} from Dropbox to Hadoop into table ${3}"
  hive -e "LOAD DATA LOCAL INPATH  '${2}' OVERWRITE INTO TABLE ${3}"

  HADOOP_OVERWRITE_RET_CODE=${?}
   # this .tmp log is to ensure that we capture all of the output to the log
   LOGGER I "`cat ${LOG_FILE}.tmp`i"
   rm -f ${LOG_FILE}.tmp
   if [ ${HADOOP_OVERWRITE_RET_CODE} -ne 0 ]
   then
      	DROPBOX_TO_HADOOP_OVERWRITE_FLAG=0
       	n=0
       	until [ $n -ge ${RETRY_COPY_NO_OF_TIMES} ]
       	do
		echo "retry $n ::"
		echo "Retrying to Copy ${1} file ${2} from Dropbox to Hadoop "
  		hive -e "LOAD DATA LOCAL INPATH  '${2}' OVERWRITE INTO TABLE ${3}"
		if [ ${?} -ne 0 ]
		then
        		n=$[$n+1]
        		sleep 15
		
		else
			echo "Copied ${1} file ${2} from Dropbox to Hadoop"
			DROPBOX_TO_HADOOP_OVERWRITE_FLAG=1
			return 1		
   		fi
       done 
 
      LOGGER E "********** ERROR **********"
      LOGGER E "Dropbox to Hadoop overwrite exited with a non-zero exit code: ${HADOOP_OVERWRITE_RET_CODE}"
      LOGGER E "Research the error, and correct the condition before restarting"
      FAILURE ${4}
   else 
      DROPBOX_TO_HADOOP_OVERWRITE_FLAG=1
   fi
	if [ "${IS_MULTICHAR_DELIM}" == "N" ]
        then
		LOGGER I "BEGIN - Refresh Data"
                REFRESH_QUERY="REFRESH \`$DATABASE_NAME\`.\`$TABLE_NAME\`"
                impala-shell --config "${BASEDIR}config/impala-config" -q "$REFRESH_QUERY"
                 if [ ${?} -ne 0 ]
                 then
                        LOGGER E "********** ERROR **********"
                        LOGGER E "Refresh Table exited with a non-zero exit code"
                        FAILURE 126
                 fi
                LOGGER I "END - Refresh Data"

	fi

        LOGGER I "End - Copy ${1} file ${2} from Dropbox to Hadoop"
}


function MOVE_FILE_TO_BACKUP_DIR(){

# function inputs
   # ${1} = variable name
   # ${2} = varibale value - fully classified localPath
   # ${3} = value - fully classified backupPath
   # ${4} = exit code to use when check fails
   #        if exit code is null, then do not fail

 LOGGER I "Begin - Move ${1} files ${2} from Dropbox to Backup ${3}"
 mv ${2} ${3}
 MOVE_TO_BACKUP_RET_CODE=${?}   
 # this .tmp log is to ensure that we capture all of the output to the log
   LOGGER I "`cat ${LOG_FILE}.tmp`i"
   rm -f ${LOG_FILE}.tmp
   if [ ${MOVE_TO_BACKUP_RET_CODE} -ne 0 ]
   then
      LOGGER E "********** ERROR **********"
      LOGGER E "Dropbox to Backup move exited with a non-zero exit code: ${MOVE_TO_BACKUP_RET_CODE}"
      LOGGER E "Research the error, and correct the condition before restarting"
      FAILURE ${4}
   
   fi
        LOGGER I "End - Move ${1} files ${2} from local to backup ${3}"

}

function REMOVE_FILES_AFTER_RETENSION_PERIOD(){
# function inputs
   # ${1} = variable name
   # ${2} = varibale value - fully classified backupPath
   # ${3} = varibale value - retension period in days
   # ${4} = exit code to use when deletion fails
   #        if exit code is null, then do not fail

LOGGER I "BEGIN - Deletion of ${1} ${2} after ${3} days"

ArchivedFilesToRemove=`find "${2}"_* -maxdepth 1 -mtime "+""${3}" -type f`
ArchivedFilesToRemoveArray=($ArchivedFilesToRemove)
if [ ${#ArchivedFilesToRemoveArray[@]} -gt 0 ]
then
	for ArchivedFiles in ${ArchivedFilesToRemoveArray[@]}; do
		LOGGER I "Removing file ${ArchivedFiles} from dropbox"
		rm -vf ${ArchivedFiles}
		if [ ${?} -ne 0 ]
		then
		    LOGGER E "********** ERROR **********"
		    LOGGER E "Deletion of files after retension period exited with a non-zero exit code"
		    FAILURE ${4}
		else
			 LOGGER I "Removed ${ArchivedFiles} successfully from Archived Directory"
		fi
	done
else
LOGGER I "No files older ${3} days to remove from archived directory for table  ${2}"
fi

LOGGER I "End - Deletion of ${1} ${2} after ${3} days"

}


function EXECUTE_KPI_VIEW_CREATION() {
# function inputs
	# ${1} = DATA_AS_OF_DAILY_DATE
	HADOOP_CLASSPATH_TMP=`hadoop classpath`
	export HADOOP_CLASSPATH=$HIVE_CP:${HADOOP_CLASSPATH_TMP}
	LOGGER I "HADOOP_CLASSPATH=$HADOOP_CLASSPATH"
	LOGGER I "BEGIN - Executing View Creation"
	java -cp ${BASEDIR}/lib/KPILiteFWFRS-0.0.1-SNAPSHOT-jar-with-dependencies.jar:$HADOOP_CLASSPATH com.ubs.frs.Driver ${DATA_AS_OF_DAILY_DATE} ${BASEDIR}/config/connectInfo.properties

	if [ ${?} -ne 0 ]
	then 
	LOGGER E "********** ERROR **********"
	LOGGER E "View Creation Failed"
	fi
	
	LOGGER I "END - Executing View Creation"
	
}

function EXTRACT_DATE_FROM_FILE_NAME(){
# function inputs
# ${1} = file name
# ${2} = file path

FILE_DATE=$( echo ${1} | grep -Eo '[[:digit:]]{8}' | head -1)
echo $FILE_DATE > ${2}file_date.txt
}

function LOAD_FILE_DATE_FROM_FILE(){
# function inputs
# ${1} = file path

FILE_DATE=$( cat ${1}file_date.txt)

}

function IDENTIFY_FILE_WITH_YYYYMMDD_POSTFIX(){
# function inputs
   # ${1} = file name
   IS_FILE_WITH_YYYYMMDD_POSTFIX=0

   FILE_NAME_TEMP=${1}
                PATTERN="YYYYMMDD"
                if echo "$FILE_NAME_TEMP" | grep -q "$PATTERN"; then
                  IS_FILE_WITH_YYYYMMDD_POSTFIX=$( echo 1 )
                fi
}

function EXTRACT_GENERIC_FILE_NAME_WITH_WILDCARD(){
# function inputs
   # ${1} = file name
echo $1 
echo 'In Wildcard'
   GENERIC_FILE_NAME_WITH_WILDCARD=$(echo ${1} | sed -E 's/_YYYYMMDD/*/g')

}


function EXTRACT_LATEST_FILE_NAME_FROM_FOLDER(){
# function inputs
   # ${1} = file name
   # ${2} = file path
echo "${GENERIC_FILE_NAME_WITH_WILDCARD}"
echo $2   
if [ "${SOURCE}" == "MANUAL" ] 
then
	LATEST_FILE_NAME_FROM_FOLDER=$(ls -rt ${2}${GENERIC_FILE_NAME_WITH_WILDCARD} |  head -1)
else
	LATEST_FILE_NAME_FROM_FOLDER=$(ls -t ${2}${GENERIC_FILE_NAME_WITH_WILDCARD} |  head -1)
fi
echo 'after latest file'
}


function ALTER_FILE_NAME_WITH_DATE(){
# function inputs
   # ${1} = file name
   # ${2} = file date
  FILENAME=$(echo ${1} | sed -E "s/_YYYYMMDD/_${2}/g")
}

function LOAD_STANDARD_DATA_SET_CONTEXT(){
#This function makes an entry into GOVERNED.STANDARD_DATA_SET_CONTEXT based on the inputs passed
#function inputs
# ${1} = Table Name
# ${2} = Layer
# ${3} = STATUS
# ${4} = DATA SET CODE


LOGGER I "Inside LOAD_STANDARD_DATA_SET_CONTEXT function"
LOGGER I "Table Name: ${1} "

LOGGER I "BEGIN - Fetching parameters from GOVERNED.STANDARD_DATA_SET_NODE"


echo "5 is ${4}"
if [ ! -z ${4} ]
then
data_set_param=$(impala-shell --config "${BASEDIR}config/impala-config" -B --output_delimiter="|" -q "SELECT NODE.DATA_SET_CODE,
       DATA_SET_DESC,
       BUSINESS_DIVISION,
       DATA_SET_DOMAIN,
       NODE.FREQUENCY_CODE,
       DATA_SET_PROCESS_TYPE,
       AS_IS_TABLE_NAME,
       GOVERNED_TABLE_NAME,
       PROVISION_TABLE_NAME,
       CASE
           WHEN NODE.DATA_SET_COB_DATE_PROCESS_TYPE='MCR' THEN EDM1.COB_DATE_STR
           ELSE COB.DATA_SET_COB_DATE
       END AS DATA_SET_COB_DATE
FROM GOVERNED.STANDARD_DATA_SET_NODE NODE
JOIN GOVERNED.STANDARD_DATA_SET_PROCESS_COB COB ON NODE.DATA_SET_CODE=COB.DATA_SET_CODE
LEFT JOIN (SELECT TABLE_NAME,FILE_NAME FROM ETL.WORK_FILE_NAME_MAPPING WHERE DATA_AS_OF_DAILY_DATE=
    (SELECT MAX(DATA_AS_OF_DAILY_DATE)
     FROM ETL.WORK_FILE_NAME_MAPPING)) FILE ON SUBSTR(FILE.TABLE_NAME,1,(INSTR(FILE.TABLE_NAME,'_Staging',1)-1))=NODE.AS_IS_TABLE_NAME
LEFT JOIN
  (SELECT MAX (COB_DATE_STR) AS COB_DATE_STR,
              SUBSTR (COB_DATE_STR, 3, 4) AS YEAR_MONTH
   FROM GOVERNED.EDS_DATE_MASTER
   GROUP BY YEAR_MONTH) EDM1 ON SUBSTR(SUBSTR(FILE_NAME,1,INSTR(FILE_NAME,'_TS')-1),-4) = EDM1.YEAR_MONTH
WHERE NODE.DATA_SET_CODE='${4}' AND (AS_IS_TABLE_NAME='${TABLE_NAME}'
       OR GOVERNED_TABLE_NAME ='${TABLE_NAME}'
       OR PROVISION_TABLE_NAME='${TABLE_NAME}')" -V --quiet)
else
data_set_param=$(impala-shell --config "${BASEDIR}config/impala-config" -B --output_delimiter="|" -q "SELECT NODE.DATA_SET_CODE,
       DATA_SET_DESC,
       BUSINESS_DIVISION,
       DATA_SET_DOMAIN,
       NODE.FREQUENCY_CODE,
       DATA_SET_PROCESS_TYPE,
       AS_IS_TABLE_NAME,
       GOVERNED_TABLE_NAME,
       PROVISION_TABLE_NAME,
       CASE
           WHEN NODE.DATA_SET_COB_DATE_PROCESS_TYPE='MCR' THEN EDM1.COB_DATE_STR
           ELSE COB.DATA_SET_COB_DATE
       END AS DATA_SET_COB_DATE
FROM GOVERNED.STANDARD_DATA_SET_NODE NODE
JOIN GOVERNED.STANDARD_DATA_SET_PROCESS_COB COB ON NODE.DATA_SET_CODE=COB.DATA_SET_CODE
LEFT JOIN (SELECT TABLE_NAME,FILE_NAME FROM ETL.WORK_FILE_NAME_MAPPING WHERE DATA_AS_OF_DAILY_DATE=
    (SELECT MAX(DATA_AS_OF_DAILY_DATE)
     FROM ETL.WORK_FILE_NAME_MAPPING)) FILE ON SUBSTR(FILE.TABLE_NAME,1,(INSTR(FILE.TABLE_NAME,'_Staging',1)-1))=NODE.AS_IS_TABLE_NAME
LEFT JOIN
  (SELECT MAX (COB_DATE_STR) AS COB_DATE_STR,
              SUBSTR (COB_DATE_STR, 3, 4) AS YEAR_MONTH
   FROM GOVERNED.EDS_DATE_MASTER
   GROUP BY YEAR_MONTH) EDM1 ON SUBSTR(SUBSTR(FILE_NAME,1,INSTR(FILE_NAME,'_TS')-1),-4) = EDM1.YEAR_MONTH
WHERE (AS_IS_TABLE_NAME='${TABLE_NAME}'
       OR GOVERNED_TABLE_NAME ='${TABLE_NAME}'
       OR PROVISION_TABLE_NAME='${TABLE_NAME}')
LIMIT 1" -V --quiet)
fi

if [ -z "$data_set_param" ]
then
echo "$data_set_param doesnt have values"
else 
IFS="|"
DATA_SET_PARAM=($data_set_param)
DATA_SET_CODE=${DATA_SET_PARAM[0]}
DATA_SET_DESC=${DATA_SET_PARAM[1]}
BUSINESS_DIVISION=${DATA_SET_PARAM[2]}
DATA_SET_DOMAIN=${DATA_SET_PARAM[3]}
FREQUENCY_CODE=${DATA_SET_PARAM[4]}
DATA_SET_PROCESS_TYPE=${DATA_SET_PARAM[5]}
AS_IS_TABLE_NAME=${DATA_SET_PARAM[6]}
GOVERNED_TABLE_NAME=${DATA_SET_PARAM[7]}
PROVISION_TABLE_NAME=${DATA_SET_PARAM[8]}
DATA_SET_COB_DATE=${DATA_SET_PARAM[9]}
#DATA_SET_COB_DATE=`echo $DATA_SET_COB_DATE1|sed 's/\\r//g'|cut -c 1-8`
OWNER=$2

if [ ${?} -ne 0 ]
then
LOGGER E "********************Error*************"
LOGGER E "Error while Fetching Parameters from GOVERNED.STANDARD_DATA_SET_NODE"
FAILURE 126
fi
LOGGER I "END - Fetching Parameters from GOVERNED.STANDARD_DATA_SET_NODE"
LOGGER I "DATA_SET_CODE is :  ${DATA_SET_CODE}"
LOGGER I "DATA_SET_DESC is :  ${DATA_SET_DESC}"
LOGGER I "BUSINESS_DIVISION is :  ${BUSINESS_DIVISION}"
LOGGER I "DATA_SET_DOMAIN is :  ${DATA_SET_DOMAIN}"
LOGGER I "FREQUENCY_CODE is :  ${FREQUENCY_CODE}"
LOGGER I "DATA_SET_PROCESS_TYPE is :  ${DATA_SET_PROCESS_TYPE}"
LOGGER I "AS_IS_TABLE_NAME is :  ${AS_IS_TABLE_NAME}"
LOGGER I "GOVERNED_TABLE_NAME is :  ${GOVERNED_TABLE_NAME}"
LOGGER I "PROVISION_TABLE_NAME is :  ${PROVISION_TABLE_NAME}"
LOGGER I "RECORD_COUNT is : ${RECORD_COUNT}"
LOGGER I "DATA_SET_COB_DATE is : ${DATA_SET_COB_DATE}"

START_TS=`date "+%Y-%m-%d %H:%M:%S"`
LOGGER I "START_TS is : $START_TS"

if [ "$OWNER" == "AS_IS" ]
then
RECORD_COUNT=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "select NVL(count(1),0) from ${OWNER}.${TABLE_NAME} WHERE DATA_AS_OF_DAILY_DATE='${DATA_AS_OF_DAILY_DATE}'" -V --quiet)
else RECORD_COUNT=0
fi

if [ ${?} -ne 0 ]
then
LOGGER E "********************Error*************"
LOGGER E "Error while getting the RECORD Count"
FAILURE 126
else LOGGER I "RECORD_COUNT is :$RECORD_COUNT"
fi

CONTEXT_ID=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "SELECT NVL(MAX(CONTEXT_ID),0) FROM GOVERNED.STANDARD_DATA_SET_CONTEXT WHERE DATA_SET_CODE='${DATA_SET_CODE}' AND RUN_DATE='${DATA_AS_OF_DAILY_DATE}'" -V --quiet)

if [ ${?} -ne 0 ]
then
LOGGER E "********************Error*************"
LOGGER E "Failed to retrieve CONTEXT_ID"
FAILURE 126
fi

if [ $CONTEXT_ID -gt 0 ]
then 
SET_CONTEXT_SEQ=$CONTEXT_ID
else
SET_CONTEXT_SEQ=`sqlplus -s ${EDS_ORCL_USER}/${EDS_ORCL_PASS}@${DATABASE_CONNECTION} <<EOF
SET HEAD OFF;
set feedback off;
select EDS_COMMON.STANDARD_DATA_SET_CONTEXT_SEQ.nextval as SEQ from dual;
EOF`

if [ ${?} -ne 0 ]
then
LOGGER E "********************Error*************"
LOGGER E "Failed to retrieve Sequence value FROM ORACLE DB"
FAILURE 126
else 
echo $SET_CONTEXT_SEQ
SET_CONTEXT_SEQ=`echo ${SET_CONTEXT_SEQ}|sed 's/\t//g'| tr -d '\n'`
LOGGER I "SET_CONTEXT_SEQ is :$SET_CONTEXT_SEQ"
fi
fi

ROW_NUMBER=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "SELECT count(1)+1 from GOVERNED.standard_data_set_context where DATA_SET_CODE='${DATA_SET_CODE}' and context_id=$SET_CONTEXT_SEQ" -V --quiet)
echo $ROW_NUMBER

impala-shell --config "${BASEDIR}config/impala-config" -q "INSERT INTO TABLE GOVERNED.STANDARD_DATA_SET_CONTEXT PARTITION (DATA_AS_OF_DATE) SELECT $ROW_NUMBER ROW_NBR,${SET_CONTEXT_SEQ} AS CONTEXT_ID,'${DATA_AS_OF_DAILY_DATE}' AS RUN_DATE,'${DATA_SET_COB_DATE}' AS DATA_SET_COB_DATE,0 VERSION_NBR,'${DATA_SET_CODE}' AS DATA_SET_CODE,'${DATA_SET_DESC}' AS DATA_SET_DESC,'${BUSINESS_DIVISION}' AS BUSINESS_DIVISION,'${DATA_SET_DOMAIN}' AS DATA_SET_DOMAIN,'${FREQUENCY_CODE}' AS FREQUENCY_CODE,'${DATA_SET_PROCESS_TYPE}' AS DATA_SET_PROCESS_TYPE,'${TABLE_NAME}' AS TABLE_NAME,'$RECORD_COUNT' RECORD_COUNT,'${OWNER}' LAYER_NAME,'$START_TS' START_TS,'$3' STATUS_DESC,'${DATA_SET_CODE}' AS DATA_AS_OF_DATE"
fi
}

#################################################################################
# Start the logical processing
#################################################################################


# Set some internal variables
PROGNAMELONG=`basename $0`                                      # name of the program, minus all of the path info
PROGNAMESHORT=`echo ${PROGNAMELONG} | sed 's/\.[^.]*$//'`       # name of the program, minus the extension
DATETIME=`date +%Y%m%d%H%M%S`                                   # datetime stamp yyyymmddhhmiss
BUSINESS_DATE=`date +"%Y%m%d"`
SYSDATE=`date +%Y%m%d%`
CURPID=$$                              # PID of the current running process (knowing this helps with concurrency)

# Before we can initialize the dynamic log management, we have to parse the
# command line and INI to set all of the varibale options.  However, we do want
# to generate a log right immediately so that we can log the process of parsing
# the command line and INI (to be able to troubleshoot if those steps fail).
# We'll start by creating a temporary log in the current working directory.
# 
# Dynamic log management will be initialized only after a successful read of the 
# command line and INI options.  Once that is ready, we'll move the log out of
# current working directory and into the directory structrue defined in the INI.

#DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TEMP_LOG_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )                                              # just the current working directory
TEMP_LOG_FILE=${TEMP_LOG_DIR}/logs/${PROGNAMESHORT}_${DATETIME}_${CURPID}.log  # temporary log   
LOG_FILE=${TEMP_LOG_FILE}
#Supress logs from Terminal
exec > $LOG_FILE
exec 2>&1
PARTITION_DIRECTORY_EMPTY=0

# Now make sure that we can actually open and write the temporary log 
touch ${LOG_FILE} > /dev/null 2>&1
if ! [ -w ${LOG_FILE} ]; then
    "********** ERROR **********"
   echo "The temporary log file ${LOG_FILE} is not writeable"
   echo "Check the location and permissions and try again"
   FAILURE 103
fi

# OK, now we can start actually logging
LOGGER I "********** BEGIN PROCESS **********"
LOGGER I "Full command line: ${0} ${*}"
LOGGER I "The current unix pid is: ${CURPID}"

# Parse the command line options provided
LOGGER I "Begin - parse command line options with getopts"
while getopts :i:s: OPTIONS
do
   case ${OPTIONS} in
      i)  INI_FILE=${OPTARG};;
      s)  INI_SECTION=${OPTARG};;
     \?)  LOGGER E "********** ERROR **********"
          LOGGER E "A required switch was missing, or an invalid switch was supplied"
          PROGRAM_USAGE
          FAILURE 102;; 
   esac
done
LOGGER I "End - parse command line options with getopts"


# check to ensure that the required -i switch for INI_FILE was provided
CHECK_NULL_VAR INI_FILE "${INI_FILE}" 101
# Confirm that the INI file specified actualy exists
CHECK_FILE_EXISTS INI_FILE "${INI_FILE}" 124

# check to ensure that the required -s switch for INI_SECTION was provided
CHECK_NULL_VAR INI_SECTION "${INI_SECTION}" 101

# Source global parameters from the INI file
LOGGER I "Begin - read GLOBAL varibales from INI_FILE"
PARSE_INI ${INI_FILE} GLOBAL
LOGGER I "End - read GLOBAL varibales from INI_FILE"

# source process-specific parameters from the INI file
LOGGER I "Begin - read process-specifc variables from INI_FILE for INI_SECTION"
PARSE_INI ${INI_FILE} ${INI_SECTION}
LOGGER I "End - read process-specific variables from INI_FILE for INI_SECTION"

# Orchestration specific parameters from the INI file
LOGGER I "Begin - read Orchestration-specifc variables from INI_FILE for GLOBAL section"
PARSE_INI ${BASEDIR}config/EDS_ORCSTRN.ini GLOBAL
LOGGER I "End - read Orchestration-specific variables from INI_FILE for GLOBAL section"

# Orchestration specific parameters from the INI file
LOGGER I "Begin - read Orchestration-specifc variables from INI_FILE for INI_SECTION"
PARSE_INI ${BASEDIR}config/EDS_ORCSTRN.ini ${INI_SECTION}
LOGGER I "End - read Orchestration-specific variables from INI_FILE for INI_SECTION"

#Initiate orchestartion process
ORCHESTRATION_STATUS RUNNING
if [ ${?} -ne 0 ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "Failed to execute JAVA API for orchestartion at RUNNING stage"
fi

# OK, now we need to validate that other expected variables have been set by the INI
# process, and that access to additional files or directories are as expected
#CHECK_NULL_VAR EMAIL "${EMAIL}" 104
CHECK_NULL_VAR LOG_RETAIN_DAYS "${LOG_RETAIN_DAYS}" 104

CHECK_NULL_VAR LOG_DIR "${LOG_DIR}" 104
# Check to ensure the log directory exists
# note - we could also decide to attempt to create it within the script
# but my fear is that if the directory disappears, that there might be a
# larger issue occurring
if ! [ -d ${LOG_DIR} ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "The LOG_DIR ${LOG_DIR} does not exist"
   LOGGER E "Ensure that a valid directory with the proper access exists"
   FAILURE 112
fi

# check to confirm that the log directory is writable
CHECK_FILE_WRITABLE LOG_DIR_WRITABLE_CHECK ${LOG_DIR}/temp_${CURPID}.log 114

# Check to ensure the archive subdirectory of the log directory exists
if ! [ -d ${LOG_DIR}/archive ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "The archive subdirectory of LOG_DIR ${LOG_DIR} does not exist"
   LOGGER E "Ensure that a valid directory with the proper access exists"
   FAILURE 113
fi

# check to ensure that the archive subdirectory of the log directory is writable
CHECK_FILE_WRITABLE LOG_DIR_ARCHIVE_WRITABLE_CHECK ${LOG_DIR}/archive/temp_${CURPID}.log 115

CHECK_NULL_VAR JOB_NAME "${JOB_NAME}" 104

CHECK_NULL_VAR LOCK_FILE "${LOCK_FILE}" 104
COPY_LOCK_FILE=${BASEDIR}lock/COPY_${PROJECT}_${LOCK_FILE}
# Intentionally not using the CHECK_FILE_EXIST function here
# In this case, I want the process to fail if the file does
# exist instead of failing when it does not
if [ -f ${COPY_LOCK_FILE} ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "The LOCK_FILE ${COPY_LOCK_FILE} already exists"
   LOGGER E "This indicates that either the previously scheduled run of"
   LOGGER E "the job is still running, or that the previous run of the job"
   LOGGER E "has failed.  The root cause needs to be researched before"
   LOGGER E "the process can be allowed to run, else you run the risk of"
   LOGGER E "messing up the High Water Mark and perhaps missing files."
 exit 1   
fi
CHECK_FILE_WRITABLE LOCK_FILE "${COPY_LOCK_FILE}" 106

# OK, now at this point all varaibles and parameters have been set
# so we'll switch the logging over to the correct log store.

LOGGER I "Begin - switching temporary log file ${TEMP_LOG_FILE} to managed structure"
LOG_FILE=${LOG_DIR}/${PROGNAMESHORT}_${JOB_NAME}_${DATETIME}_${CURPID}.log
mv -f ${TEMP_LOG_FILE} ${LOG_FILE} 
LOGGER I "The LOG_FILE is now being written to ${LOG_FILE}"
LOGGER I "End - switching temporary LOG to managed structure"

# Now here is where we will actually begin the processing

# create a LOCK file
LOGGER I "Begin - Creating LOCK_FILE ${COPY_LOCK_FILE}"
touch ${COPY_LOCK_FILE}
if [ ${?} -ne 0 ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "could not create LOCK_FILE ${COPY_LOCK_FILE}"
   FAILURE 117
fi
LOGGER I "End - Creating LOCK_FILE ${COPY_LOCK_FILE}"


<<KERBEROS_COMMENT
#########################################################################################
#             Kerberos Authentication
#
########################################################################################
KERBEROS_PROPERTIES_FILE=${BASEDIR}/config/kerberos_param.properties
source $KERBEROS_PROPERTIES_FILE
# Kerberos authentication using kinit command
kinit "$KERBEROS_USER"@"$KERBEROS_REALM" -k -t $KEYTAB_FILE 
ERROR_RET_CODE=${?}
if [ ${ERROR_RET_CODE} -ne 0 ]
then 
	LOGGER E "******Error**********"
	LOGGER E "Error occured in Kerberos Authentication - $ERROR_RET_CODE "
	FAILURE 135
fi
KERBEROS_COMMENT

#Archived Dropbox Path
DROPBOX_ARCHIVED_PATH="${DROPBOX_PATH}/${BACKUP_DIR}"
#DropBox Input Path
DROPBOX_INPUT_PATH=${DROPBOX_PATH}/${INPUT_DIR}${FILENAME}
#BUSINESS_DATE='20151002'
DROPBOX_DIR_PATH=${DROPBOX_PATH}/${INPUT_DIR}
file_consolidated=${FILENAME} #For consolidated input file MCR
declare -a file_name #To store multiple MCR input files for consolidation and archival
file_count=0 #To store number of MCR input file for ingestion
LOGGER I "Global variable file_consolidated is created with value- ${file_consolidated} for merging multiple MCR source files"
LOGGER I "BEGIN - Fetching EDS Batch Date from GOVERNED.EDS_BATCH_DATE_FILE"


DATEFILE=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "SELECT COB_DATE_STR, COB_DATE_MINUS_1_STR, COB_DATE_MINUS_2_STR, COB_MONTH_STR, QTD_INT, YTD_FIRST_DAY_STR, YTD_MINUS_1_FIRST_DAY_STR, YTD_INT, YTD_MINUS_1_INT, COB_MONTH_LAST_DAY_STR, YTD_LAST_DAY_STR, YTD_PLUS_1_INT, COB_DATE_MINUS_3_STR,from_unixtime(unix_timestamp(SUBSTR(CAST(DATE_SUB(ytd_minus_2_last_day_dt,10) AS STRING),1,10),'yyyy-MM-dd'),'yyyyMMdd') AS YTD_MINUS_2_LAST_7_DAY_STR,BUSINESS_DAY, MONTH_PRIOR_1_LAST_DAY_STR,COB_DATE_PLUS_1_STR FROM GOVERNED.EDS_BATCH_DATE_FILE" -V --quiet) 

#Below Exception Handling is added for JIRA 10850 
if [ ${?} -ne 0 ]
then
LOGGER E "********************Error*************"
LOGGER E "Error while Fetching EDS Batch Date from GOVERNED.EDS_BATCH_DATE_FILE"
FAILURE 126
fi

IFS="	"
datefile=($DATEFILE)
DATA_AS_OF_DAILY_DATE=${datefile[0]}
DATA_AS_OF_DAILY_DATE_MINUS_1=${datefile[1]}
DATA_AS_OF_DAILY_DATE_MINUS_2=${datefile[2]}
DATA_AS_OF_YEAR_MONTH=${datefile[3]}
DATA_AS_OF_QUARTER=${datefile[4]}
DATA_AS_OF_YEAR_FIRST_DAY=${datefile[5]}
DATA_AS_OF_YEAR_FIRST_DAY_MINUS_1=20170102
DATA_AS_OF_YEAR=${datefile[7]}
DATA_AS_OF_YEAR_MINUS_1=${datefile[8]}
DATA_AS_OF_LAST_DAY_OF_MONTH=${datefile[9]}
DATA_AS_OF_LAST_DAY_OF_YEAR=${datefile[10]}
DATA_AS_OF_LAST_DAY_PREVIOUS_YEAR=${datefile[11]}
DATA_AS_OF_DAILY_DATE_MINUS_3=${datefile[12]}
DATA_AS_OF_LAST_DAY_OF_YEAR_MINUS_7=${datefile[13]}
BUSINESS_DAY=${datefile[14]}
DATA_AS_OF_LAST_DAY_OF_LAST_MONTH=${datefile[15]}
COB_DATE_PLUS_1_STR=${datefile[16]}

#Below Lines are commented for JIRA 10850
#if [ ${?} -ne 0 ]
#then
#LOGGER E "********************Error*************"
#LOGGER E "Error while Fetching EDS Batch Date from GOVERNED.EDS_BATCH_DATE_FILE"
#FAILURE 126
#fi
LOGGER I "END - Fetching EDS Batch Date from GOVERNED.EDS_BATCH_DATE_FILE"
LOGGER I "BUSINESS_DAY in DATE File is :  ${BUSINESS_DAY}"
LOGGER I "DATA_AS_OF_LAST_DAY_PREVIOUS_MONTH is: ${DATA_AS_OF_LAST_DAY_PREVIOUS_MONTH}"

INI_SECTION_NAME=`echo $INI_SECTION | sed 's/\[//g'| sed 's/\]//g'`
if [ ${INI_SECTION_NAME} == "KPI_VIEW_CREATION"  ]
then
	LOGGER "Value set for INI_SECTION is ${INI_SECTION_NAME}"
	EXECUTE_KPI_VIEW_CREATION
	if [ ${?} -ne 0 ]
	then
		FAILURE 130
	fi

else
	LOGGER I "Begin - Executing Non-KPI_VIEW_CREATION of ${INI_SECTION_NAME}"

# Identify if the file has YYYYMMDD patttern
IDENTIFY_FILE_WITH_YYYYMMDD_POSTFIX ${FILENAME}
LOGGER I "After IDENTIFY_FILE_WITH_YYYYMMDD_POSTFIX "


if [ ${IS_FILE_WITH_YYYYMMDD_POSTFIX} == "1" ]
then
        LOGGER  I "Filename has generic YYYYMMDD postfix ${FILENAME}"
        echo "FILENAME: " $FILENAME
        EXTRACT_GENERIC_FILE_NAME_WITH_WILDCARD ${FILENAME}
        EXTRACT_LATEST_FILE_NAME_FROM_FOLDER ${GENERIC_FILE_NAME_WITH_WILDCARD} ${DROPBOX_DIR_PATH}
        DROPBOX_INPUT_PATH=${LATEST_FILE_NAME_FROM_FOLDER}
        LOGGER I "File pattern ${GENERIC_FILENAME}, Latest file in directory ${DROPBOX_INPUT_PATH}"
        EXTRACT_DATE_FROM_FILE_NAME "${DROPBOX_INPUT_PATH}" "${DROPBOX_DIR_PATH}"
 	LOGGER I "The file date is ${FILE_DATE}"
	ALTER_FILE_NAME_WITH_DATE ${FILENAME} ${FILE_DATE}
	LOGGER I "New filename ${FILENAME}"

fi


#If Phase is Staging and Ingest Flag is 'Y' then we load the file to ASIS Staging Table
if [  "${PHASE}" == "Staging"  ] && [ "${COPY_FILE_FLAG}" == "Y" ]
then
if [ "${SOURCE}" == "MCR" ] 
then
         LOGGER I "MCR Source System- update DROPBOX_INPUT_PATH"
#Replace YYYYMM with last month
#PREV_DATE_MONTH=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "SELECT substr(month_prior_1_first_day_str,1,6) FROM GOVERNED.EDS_BATCH_DATE_FILE" -V --quiet)
#file_name_new=$(echo "${FILENAME}" | sed -e "s/YYYYMM/${PREV_DATE_MONTH}/g")
file_name_new=$(echo "${FILENAME}")
         DROPBOX_INPUT_PATH=${DROPBOX_PATH}/${INPUT_DIR}${file_name_new}


LOGGER I "DROPBOX_INPUT_PATH for MCR is- ${DROPBOX_INPUT_PATH}"

else
###Incase of multiple files pick the first file
	FILENAME=`basename $(ls ${DROPBOX_DIR_PATH}/${FILENAME}|head -1)`
	DROPBOX_INPUT_PATH=${DROPBOX_DIR_PATH}/${FILENAME}
	CHECK_FILE_EXISTS INPUT_FILE "${DROPBOX_INPUT_PATH}" 136 
	#Check If Input File is of Zero Bytes. If size is Zero Bytes, then Make a entry in zeroBytesFiles.txt and exit.
fi
        find ${DROPBOX_INPUT_PATH} -maxdepth 1 -empty > ${BASEDIR}zeroBytesFiles.txt
	while read line; do
	  LOGGER E "File $line is of zero bytes"
	FAILURE 130
	done <${BASEDIR}zeroBytesFiles.txt
        LOGGER I "BEGIN - Overwriting files in  Staging Directory"
LOGGER I " Outside the MERGE Function: Full input file path : ${DROPBOX_INPUT_PATH}  Full input directory path :${DROPBOX_DIR_PATH}  Full input file name : ${file_name_new}"


     #Check if source is MCR
     if [ "${SOURCE}" == "MCR" ] && [ "${TABLE_NAME}" != "MCR_LRD_ACTUAL_50_058_GROUP_Staging" ] && [ "${TABLE_NAME}" != "MCR_LRD_RESTATEMENT_50_058_GROUP_Staging" ] && [ "${TABLE_NAME}" != "MCR_LRD_ACTUAL_50_058_STANDALONE_Staging" ] && [ "${TABLE_NAME}" != "MCR_LRD_RESTATEMENT_50_058_STANDALONE_Staging" ] && [ "${TABLE_NAME}" != "MCR_LRD_ACTUAL_50_058_DEDUCTIONS_Staging" ] && [ "${TABLE_NAME}" != "MCR_LRD_RESTATEMENT_50_058_DEDUCTIONS_Staging" ] && [ "${TABLE_NAME}" != "MCR_RWA_ACTUAL_50_035_PART1_I9_Staging" ] && [ "${TABLE_NAME}" != "MCR_RWA_ACTUAL_50_035_PART2_Staging" ] && [ "${TABLE_NAME}" != "MCR_RWA_RESTATEMENT_50_035_PART1_I9_Staging" ] && [ "${TABLE_NAME}" != "MCR_RWA_RESTATEMENT_50_035_PART2_Staging" ]
     then
     LOGGER I "Executing functionalities for MCR"
     SOURCE_FILE_MERGE "${DROPBOX_INPUT_PATH}" "${DROPBOX_DIR_PATH}" "${file_name_new}" "${TABLE_NAME}" 136
     DROPBOX_INPUT_PATH=${DROPBOX_DIR_PATH}/${file_consolidated} #update DROPBOX_INPUT_PATH to point to consolidated file only
     elif [ "${SOURCE}" == "MCR" ] && [ "${TABLE_NAME}" == "MCR_RWA_ACTUAL_50_035_PART1_I9_Staging" -o "${TABLE_NAME}" == "MCR_RWA_ACTUAL_50_035_PART2_Staging" -o "${TABLE_NAME}" == "MCR_RWA_RESTATEMENT_50_035_PART1_I9_Staging" -o "${TABLE_NAME}" == "MCR_RWA_RESTATEMENT_50_035_PART2_Staging" ]     
    then
     LOGGER I "Executing functionalities for MCR RWA"
     SOURCE_FILE_MERGE_RWA "${DROPBOX_INPUT_PATH}" "${DROPBOX_DIR_PATH}" "${file_name_new}" "${TABLE_NAME}" 136
     DROPBOX_INPUT_PATH=${DROPBOX_DIR_PATH}/${FILENAME} #update DROPBOX_INPUT_PATH to point to consolidated file only
elif [ "${SOURCE}" == "MCR" ] && [ "${TABLE_NAME}" == "MCR_LRD_ACTUAL_50_058_GROUP_Staging" -o "${TABLE_NAME}" == "MCR_LRD_RESTATEMENT_50_058_GROUP_Staging" -o "${TABLE_NAME}" == "MCR_LRD_ACTUAL_50_058_STANDALONE_Staging" -o "${TABLE_NAME}" == "MCR_LRD_RESTATEMENT_50_058_STANDALONE_Staging" -o "${TABLE_NAME}" == "MCR_LRD_ACTUAL_50_058_DEDUCTIONS_Staging" -o "${TABLE_NAME}" == "MCR_LRD_RESTATEMENT_50_058_DEDUCTIONS_Staging" ]
    then
     LOGGER I "Executing functionalities for MCR LRD"
     SOURCE_FILE_MERGE_LRD "${DROPBOX_INPUT_PATH}" "${DROPBOX_DIR_PATH}" "${file_name_new}" "${TABLE_NAME}" 136
     DROPBOX_INPUT_PATH=${DROPBOX_DIR_PATH}/${FILENAME} #update DROPBOX_INPUT_PATH to point to consolidated file only
     fi
     LOGGER I "inputs to OVERWRITE_FILE_FROM_DROPBOX_TO_HADOOP function- ${DROPBOX_INPUT_PATH} ${DATABASE_NAME}.${TABLE_NAME}"
     OVERWRITE_FILE_FROM_DROPBOX_TO_HADOOP File "${DROPBOX_INPUT_PATH}" "${DATABASE_NAME}"."${TABLE_NAME}" 127
     LOGGER I "END - Overwriting files in Staging Directory"

fi
LOAD_FILE_DATE_FROM_FILE "${DROPBOX_DIR_PATH}"
LOGGER I "The FILE_DATE loaded from saved file is ${FILE_DATE}"

#If Phase is Governed and Copy Flag is 'Y', we need to execute DMLS

if ([ "${PHASE}" == "Governed" ] || [ "${PHASE}" == "Etl" ]  || [ "${PHASE}" == "Provision" ] ) && [ "${COPY_FILE_FLAG}" == "Y" ]
then
	LOAD_STANDARD_DATA_SET_CONTEXT ${TABLE_NAME} ${OWNER} 'RUNNING' ${5}
        # commenting out partition removing logic..TODO: PLEASE UNCOMMENT & CORRECT THE LOGIC

<<PARTITIONCOMMENT1
        HADOOP_PATH=${HDFS_FILE_LOCATION}
        LOGGER I "BEGIN - Check If Partition Directory exists"
        PARTITION_DIRECTORY=${HADOOP_PATH}/${PARTITION_COLUMN,,}\="${BUSINESS_DATE}/"
        echo ${PARTITION_DIRECTORY}
        hadoop fs -ls ${PARTITION_DIRECTORY} > /dev/null 2>&1
        if [ ${?} -ne 0 ]
        then
          LOGGER I " Partition Directory does not exist"
          LOGGER I "END - Check If Partition Directory exists"

        else
          LOGGER W " Partition Directory already exists"
          LOGGER I "END - Check If Partition Directory exists"

          LOGGER I "BEGIN - Remove files from Partition directory"
          REMOVE_HDFS_FILES PartionDirectoryFiles  ${PARTITION_DIRECTORY}
          LOGGER I "END - Remove files from Partition directory"
        fi
PARTITIONCOMMENT1


        # TODO: Delete below HDFS directory purge code once Partition purge is in effect -- Removed Already, COmment left for historical purposes



        LOGGER I "BEGIN - Inserting Data in Base Table ${TABLE_NAME}"
        #EXECUTE_SPARK_SQL_DML ${TABLE_NAME} ${DATABASE_NAME} ${BASEDIR}${DML_PATH}${PROJECT}/${OWNER}/${SOURCE}/${TABLE_NAME}.hql ${PHASE}
        EXECUTE_DML DMLFiles ${BASEDIR}${DML_PATH}${PROJECT}/${OWNER}/${SOURCE}/${TABLE_NAME}.hsql ${PHASE}
        LOGGER I "END - Inserting Data in Table ${TABLE_NAME}"
	LOAD_STANDARD_DATA_SET_CONTEXT ${TABLE_NAME} ${OWNER} 'COMPLETED' ${5}
fi




#If Phase is Base or Governed and Copy Flag is 'Y', we need to execute DMLS
if [ "${PHASE}" == "Base" ] && [ "${COPY_FILE_FLAG}" == "Y" ]	
then

	# commenting out partition removing logic..TODO: PLEASE UNCOMMENT & CORRECT THE LOGIC

<<PARTITIONCOMMENT1
	HADOOP_PATH=${HDFS_FILE_LOCATION}
	LOGGER I "BEGIN - Check If Partition Directory exists"
	PARTITION_DIRECTORY=${HADOOP_PATH}/${PARTITION_COLUMN,,}\="${BUSINESS_DATE}/"	
	echo ${PARTITION_DIRECTORY}
	hadoop fs -ls ${PARTITION_DIRECTORY} > /dev/null 2>&1
	if [ ${?} -ne 0 ]
	then
	  LOGGER I " Partition Directory does not exist"
          LOGGER I "END - Check If Partition Directory exists"

	else
	  LOGGER W " Partition Directory already exists"
          LOGGER I "END - Check If Partition Directory exists"

          LOGGER I "BEGIN - Remove files from Partition directory"
          REMOVE_HDFS_FILES PartionDirectoryFiles  ${PARTITION_DIRECTORY}
          LOGGER I "END - Remove files from Partition directory"	
	fi
PARTITIONCOMMENT1
#Function call to insert data in STANDARD_DATA_SET_CONTEXT for Orchestration
	LOAD_STANDARD_DATA_SET_CONTEXT ${TABLE_NAME} ${OWNER} 'RUNNING' ${5}
CONTEXT_ID=$(impala-shell --config "${BASEDIR}config/impala-config" -B -q "SELECT DISTINCT CONTEXT_ID FROM GOVERNED.STANDARD_DATA_SET_CONTEXT WHERE TABLE_NAME='${TABLE_NAME}'  AND LAYER_NAME='${OWNER}' AND RUN_DATE='${DATA_AS_OF_DAILY_DATE}'" -V --quiet)
echo "Context_id is ${CONTEXT_ID}"

	# TODO: Delete below HDFS directory purge code once Partition purge is in effect -- Removed Already, COmment left for historical purposes
        LOGGER I "BEGIN - Inserting Data in Base Table ${TABLE_NAME}"
	#EXECUTE_SPARK_SQL_DML ${TABLE_NAME} ${DATABASE_NAME} ${BASEDIR}${DML_PATH}${PROJECT}/${OWNER}/${SOURCE}/${TABLE_NAME}.hql ${PHASE}
	EXECUTE_DML DMLFiles ${BASEDIR}${DML_PATH}${PROJECT}/${OWNER}/${SOURCE}/${TABLE_NAME}.hsql ${PHASE}
	LOGGER I "END - Inserting Data in Table ${TABLE_NAME}"
	LOAD_STANDARD_DATA_SET_CONTEXT ${TABLE_NAME} ${OWNER} 'COMPLETED' ${5}

fi

#After Loading File to AS_IS Staging Table, File is moved to Dropbox Archived Directory
if [ "${DROPBOX_TO_HADOOP_OVERWRITE_FLAG}" == 1 ]
then
	if ! [ -d ${DROPBOX_ARCHIVED_PATH} ]
	then
	   LOGGER W "The BACKUP_DIR ${DROPBOX_ARCHIVED_PATH} does not exist"
	   LOGGER I "BEGIN - Creating Archived Directory"
	   mkdir -p ${DROPBOX_ARCHIVED_PATH}
		if [ ${?} -ne 0 ]
		then
		  LOGGER E "********** ERROR **********"
		  LOGGER E "Could not create Archived directory"
		  FAILURE 132
		fi
	  LOGGER I "End - Creating Archived Directory"
	fi

 if [ "${SOURCE}" == "MCR" ]
then
 LOGGER I "BEGIN - Moving MCR Files to Dropbox Archived Directory"
#Archive consolidated input file first
TABLE_PREFIX=`echo ${TABLE_NAME}|awk -F '_' '{print $2}'`
if [ "${TABLE_PREFIX}" == "WMPC" ]
then
cntl_file=`echo ${DROPBOX_DIR_PATH}/$FILENAME|awk -F '.' '{print $1".cntl"}'`
echo "Control File is $cntl_file"
else
cntl_file_name=`basename $(ls ${DROPBOX_DIR_PATH}/${FILENAME}|awk -F'TS' '{print $1;}')`
cntl_file="$(find "${DROPBOX_DIR_PATH}" -maxdepth 1 -name "${cntl_file_name}*i*")"
echo "Control File is $cntl_file"
fi

if [ -e "${cntl_file}" ]
then
MOVE_FILE_TO_BACKUP_DIR FILE_NAME "${cntl_file}" "${DROPBOX_ARCHIVED_PATH}${file_consolidated}i_${BUSINESS_DATE}.txt" 128
else
LOGGER I "Control file doesn't exist for this load"
fi
MOVE_FILE_TO_BACKUP_DIR FILE_NAME "${DROPBOX_INPUT_PATH}" "${DROPBOX_ARCHIVED_PATH}${file_consolidated%%.*}_${BUSINESS_DATE}.txt" 128
#  DROPBOX_INPUT_PATH=${DROPBOX_PATH}/${INPUT_DIR}${file_name_new} #Reset DROPBOX_INPUT_PATH to archive all the input files.
if [ $file_count -gt 1 ]
then
X=0
LOGGER I "File name in array at $X position- ${file_name[$X]}"
TEST_VAR=${file_name[$X]}
LOGGER I " TEST_VAR= ${TEST_VAR} "
file_count_new=`expr $file_count - 1` #file_count global variable contains number of input files for MCR
for (( X=0; X<=${file_count_new}; X++ )) #loop to archive all the files
do
TEMP_VAR=${file_name[$X]}
ARCH_FILE=`basename ${TEMP_VAR}`
DROPBOX_INPUT_PATH=${DROPBOX_PATH}/${INPUT_DIR}${ARCH_FILE} #Reset DROPBOX_INPUT_PATH 
LOGGER I "Archiving file- ${DROPBOX_INPUT_PATH}"
MOVE_FILE_TO_BACKUP_DIR FILE_NAME "${DROPBOX_INPUT_PATH}" "${DROPBOX_ARCHIVED_PATH}${ARCH_FILE%%.*}_${BUSINESS_DATE}.txt" 128
LOGGER I "END - Moving MCR Files to Dropbox Archived Directory"
done
fi
else  
#Moving Dropbox files to Archived Folder
 LOGGER I "BEGIN - Moving File to Dropbox Archived Directory"
MOVE_FILE_TO_BACKUP_DIR FILE_NAME "${DROPBOX_INPUT_PATH}" "${DROPBOX_ARCHIVED_PATH}${FILENAME%%.*}_${BUSINESS_DATE}.${FILENAME##*.}" 128
  LOGGER I "END - Moving File to Dropbox Archived Directory"
	  #Removing Files from backup after Retension Period
	  REMOVE_FILES_AFTER_RETENSION_PERIOD BACKUP_FILES "${DROPBOX_ARCHIVED_PATH}${FILENAME%%.*}" "${BACKUP_RETENSION_PERIOD}" 129
fi
fi
fi	# Execute KPI View Creation end

# Processing has completed successfully, remove the lock file
LOGGER I "Begin - removing LOCK FILE ${COPY_LOCK_FILE}"
rm -rf ${COPY_LOCK_FILE} >/dev/null
if [ ${?} -ne 0 ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "could not remove LOCK_FILE ${COPY_LOCK_FILE}"
   FAILURE 116
fi
LOGGER I "End - removing LOCK FILE ${COPY_LOCK_FILE}"



LOGGER I "Close log file and move into tar: ${LOG_DIR}/archive/${PROGNAMESHORT}_${JOB_NAME}_`date +%Y%m%d`.tar"
LOGGER I "********** END PROCESS **********"

# Upon successful completion of this script, we'll move the log file to the archive directory
# Because we're finalizing the logging, the last cleanup step will be done without logging
# Because multiple threads of this process may multiple times per day,
# the logs in the archive directory will be maintained in a tar ball by date
cd ${LOG_DIR}
gzip `basename ${LOG_FILE}`
tar -rf ${LOG_DIR}/archive/${PROGNAMESHORT}_${JOB_NAME}_`date +%Y%m%d`.tar `basename ${LOG_FILE}`.gz
if [ ${?} -eq 0 ]
then
   # Remove the untarred log file once it's been successfully added to the archive tar
   # if for some reason the tar command fails, the untarred log file will remain in LOG_DIR
   rm -rf ${LOG_FILE}.gz
fi

#Finishing orchestartion process
ORCHESTRATION_STATUS FINISHED
if [ ${?} -ne 0 ]
then
   LOGGER E "********** ERROR **********"
   LOGGER E "Failed to execute JAVA API for orchestartion at FINISHED stage"
fi

exit 0
