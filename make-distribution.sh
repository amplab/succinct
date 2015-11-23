#!/usr/bin/env bash

# Script to create zip distribution for Spark Packages
##

VERSION=0.1.7-SNAPSHOT
SUCCINCT_HOME="$(cd "`dirname "$0"`"; pwd)"
DIST_DIR="$SUCCINCT_HOME/dist"
JAR_FILE=succinct-${VERSION}.jar
POM_FILE=succinct-${VERSION}.pom
ZIP_FILE=succinct-${VERSION}.zip

if [ ! -f ${SUCCINCT_HOME}/assembly/target/${JAR_FILE} ]; then
	echo "Building Succinct..."
	# Assume testing is already done
	mvn clean -DskipTests package
fi

# Create distribution directory
rm -rf ${DIST_DIR}
mkdir -p ${DIST_DIR}

# Copy jar and pom file
echo "Creating distribution ZIP..."
cp ${SUCCINCT_HOME}/assembly/target/${JAR_FILE} ${DIST_DIR}/${JAR_FILE}
cp ${SUCCINCT_HOME}/pom.xml ${DIST_DIR}/${POM_FILE}
zip -9 -j -D ${DIST_DIR}/${ZIP_FILE} ${DIST_DIR}/${JAR_FILE} ${DIST_DIR}/${POM_FILE}
echo "Distribution successfully created."
