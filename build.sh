#!/bin/sh

if [ -z "$1" ]; then
	# write error message
        echo "spqr build and deployment script"
	echo "usage: build.sh <spqr git folder> <spqr destination folder>"
	exit 1
fi

if [ -z "$2" ]; then
	# write error message
        echo "spqr build and deployment script"
	echo "usage: build.sh <spqr git folder> <spqr destination folder>"
	exit 1
fi

SRC_FOLDER=$1
DEST_FOLDER=$2

echo "source folder: $SRC_FOLDER, destination folder: $DEST_FOLDER"

# change to spqr parent project and build & deploy it to repository
cd $SRC_FOLDER/spqr-parent
mvn clean; mvn install;

# change to spqr build project and execute build & deployment
cd $SRC_FOLDER/spqr-build
mvn clean; mvn install;

# create destination folders
mkdir -p $DEST_FOLDER/spqr-node/lib
mkdir -p $DEST_FOLDER/spqr-node/bin
mkdir -p $DEST_FOLDER/spqr-node/etc
mkdir -p $DEST_FOLDER/spqr-node/repo
mkdir -p $DEST_FOLDER/spqr-node/log
mkdir -p $DEST_FOLDER/spqr-node/queues

# copy configuration from processing node project to destination folder
cp $SRC_FOLDER/spqr-node/src/main/config/* $DEST_FOLDER/spqr-node/etc/

# copy libraries from processing node project to destination folder (remove log4j-over-slf4j jar first)
rm $SRC_FOLDER/spqr-node/target/lib/log4j-over-slf4j-*.jar
cp $SRC_FOLDER/spqr-node/target/lib/* $DEST_FOLDER/spqr-node/lib/

# copy scripts from processing node project to destination folder
cp $SRC_FOLDER/spqr-node/src/main/scripts/* $DEST_FOLDER/spqr-node/bin/

echo "Build and Deployment finished"