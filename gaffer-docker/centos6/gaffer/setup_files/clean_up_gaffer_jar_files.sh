#!/bin/bash

########################################################################
#  Builds jar files based after changes to source code
#  
#######################################################################



# Directory where gaffer source code is cloned to. 
export INSTALL_DIR=/Gaffer_Source

# Directory where jar files generated by the maven build are transferred to
export TARGET_DIR=/data/Gaffer/Source

# Directory where jar files used to test examples are located
export TEST_DIR=/data/Gaffer/Test

# Directory where gaffer scripts are located
export SCRIPT_DIR=/data/Gaffer/Script

# Directory where accumulo dependency jar files are located

export JAR_FILE_DIR=/jar_files

# Directory where dependency jars for gaffer are located

export DEPENDENCIES_JAR_DIR=/data/Gaffer/jar_files


# Remove jar files generated  by  maven build
echo "Removing jar files generated by maven file"

cd /data/Gaffer

rm -f ${TARGET_DIR}/*.jar ${TARGET_DIR}/*.tar

#  Remove Gaffer example jar files

echo "Removing  Gaffer example jar files"


rm -f ${TEST_DIR}/*.jar ${TEST_DIR}/*.tar

# Remove accumulo dependency jar files

rm -f ${JAR_FILE_DIR}/*.tgz ${JAR_FILE_DIR}/*.jar

rm -f ${DEPENDENCIES_JAR_DIR}/*.jar  ${DEPENDENCIES_JAR_DIR}/*.tgz

echo "Script completed"
