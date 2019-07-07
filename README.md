#!/bin/bash

PS4='$LINENO: '
set -x

FILENAME='FRS_A_BS*'
DROPBOX_ARCHIVED_PATH='/home/karthik/IdeaProjects/Experiment/src/ArchiveData'
function fnMergeFiles() {
    #This merges Part1 RWA files by removing 26 lines of header & merges Part2 files by removing 20 lines header
    #This merges BS, LRD, PNL, HC by removing 2 lines of header
    #function inputs
    # ${1} = full input file path
    # ${2} = input directory path
    # ${3} = input file name
    # ${4} = Table name
    # ${5} = error code in case input file

    echo "File Merge Operation is starting"
    echo "Full input file path : ${1} Full input directory path : ${2} Full input file name : ${3}"

    cd ${2}
    strFileLoadType=`echo ${3} | cut -c 7-8`
    strTableName=${4}

    intFileStartNumber=1
    intHeaderCount=1
    bolDateBasedLoad=true
    strWatcherFileType="i"
    if ([ $strTableName = "MCR_RWA_ACTUAL1" ] || [ $strTableName = "MCR_RWA_RESTATEMENT1" ]) && [ "$strFileLoadType" == "RW" ]
    then
        intFileStartNumber=1
        intHeaderCount=26
    elif ([ $strTableName = "MCR_RWA_ACTUAL2" ] || [ $strTableName = "MCR_RWA_RESTATEMENT2" ]) && [ "$strFileLoadType" == "RW" ]
    then
        intFileStartNumber=51
        intHeaderCount=20
    elif ([ $strTableName = "MCR_LRD_ACTUAL1" ] || [ $strTableName = "MCR_LRD_RESTATEMENT1" ]) && [ "$strFileLoadType" == "LR" ]
    then
        intFileStartNumber=1
        intHeaderCount=2
    elif ([ $strTableName = "MCR_LRD_ACTUAL2" ] || [ $strTableName = "MCR_LRD_RESTATEMENT2" ]) && [ "$strFileLoadType" == "LR" ]
    then
        intFileStartNumber=51
        intHeaderCount=2
    elif ([ $strTableName = "MCR_LRD_ACTUAL3" ] || [ $strTableName = "MCR_LRD_RESTATEMENT3" ]) && [ "$strFileLoadType" == "LR" ]
    then
        intFileStartNumber=91
        intHeaderCount=2
    elif ([ $strTableName = "MCR_BS_ACTUAL" ] || [ $strTableName = "MCR_BS_RESTATEMENT" ] || \
 [ $strTableName = "MCR_PNL_ACTUAL" ] || [ $strTableName = "MCR_PNL_RESTATEMENT" ] || \
 [ $strTableName = "MCR_HC_ACTUAL" ] || [ $strTableName = "MCR_HC_RESTATEMENT" ] || \
 [ $strTableName = "MCR_CA_ACTUAL" ] || [ $strTableName = "MCR_CA_RESTATEMENT" ] || \
 [ $strTableName = "MCR_SI_ACTUAL" ] || [ $strTableName = "MCR_SI_RESTATEMENT" ])
    then
        intFileStartNumber=1
        intHeaderCount=2
    else
        intFileStartNumber=1
        intHeaderCount=1
        bolDateBasedLoad=false
    fi
    if [ $strWatcherFileType == "i" ]; then
        strWatcherFilePattern="${3}i"
    else
        strWatcherFilePattern="${3}"
    fi

    strIFileList=`ls -rt | egrep -E "${strWatcherFilePattern}"`
    echo "Watcher File search pattern is ${3}i"
    while read -r line; do
        echo "Starting the merging process for the Watcher file $line"
        if [ $bolDateBasedLoad ]; then
            intFileEndNumber=`echo ${line} | cut -c 9-10`
            intExpectedFileCount=$(( (intFileEndNumber - $intFileStartNumber) + 1 ))
            intFileDate=`echo ${line} | awk -F '_' '{print $(NF-2)}'`
            intActualFileCount=`ls -rt | egrep -E "${3}" | egrep -E 'FRS_.{7}'${intFileDate}'.{18}$' | wc -l`

            if [ $intActualFileCount == $intExpectedFileCount ]; then
                strDataFileList=`ls -rt | egrep -E "${3}" | egrep -E 'FRS_.{7}'${intFileDate}'.{18}$'`
                strMergedFileName=`echo $(echo ${3} | cut -c 1-8)_$(date +"%Y%m%d%H%M%S")`
                while read -r strTempFileName; do
                    sed -e '1,'${intHeaderCount}'d' ${strTempFileName} >> ${strMergedFileName}
                    mv ${strTempFileName} $DROPBOX_ARCHIVED_PATH
                    mv ${line} $DROPBOX_ARCHIVED_PATH
                done <<<"$strDataFileList"
            else
                echo "Warning: The number of files expected based on the watcher file $line is not met. Hence not loading any data for this watcher file"
            fi
        else
            strDataFileList=`ls -rt | egrep -E "${3}"`
            while read -r strTempFileName; do
                strMergedFileName=`echo $(echo ${strTempFileName} | sed 's/\.[^.]*$//')_$(date +"%Y%m%d%H%M%S")`
                sed -e '1,'${intHeaderCount}'d' ${strTempFileName} >> strMergedFileName
                mv ${line} $DROPBOX_ARCHIVED_PATH
            done <<<"$strDataFileList"
        fi
        break
    done <<<"$strIFileList"


}

fnMergeFiles '/home/karthik/IdeaProjects/Experiment/src/Source_Files/FRS_A_BS*' '/home/karthik/IdeaProjects/Experiment/src/Source_Files/' 'FRS_A_RW((0[1-9])|[1-4][0-9]|50).*' 'MCR_RWA_ACTUAL1'
