#!/bin/bash

# build all-in-one jar package

dist_dir="../dist"
dist_jar="$dist_dir/kogiri.jar"
allinone_dist_jar="$dist_dir/kogiri-all.jar"

change_dir() {
	cd ..
}

build_jar() {
    ant
}

build_jar_allinone() {
    ant allinone
}


echo "Build Kogiri all-in-one jar package"
if [ -f $allinone_dist_jar ]
then
    echo "all-in-one package already exists!"
    exit 2
fi

# move to parent
change_dir

if [ ! -f $dist_jar ]
then
    # build jar first
    build_jar
fi

if [ $? == 0 ]
then
    # build all-in-one jar next
    build_jar_allinone
fi

if [ $? == 0 ]
then
    echo "Building all-in-one jar package succeeded!"
    echo "You can find jar package in a ../dist directory"
else
    echo "Building all-in-one jar package failed!"
fi

