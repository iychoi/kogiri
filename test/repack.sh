#!/bin/bash

# run preprocess

dist_dir="dist"
dist_jar="$dist_dir/kogiri.jar"
allinone_dist_jar="$dist_dir/kogiri-all.jar"
build_script="scripts/package_allinone.sh"

change_dir() {
    if [[ $PWD == *"/test" ]]
    then
    	cd ..
	fi
}

# move to right directory
change_dir

if [ -f $dist_jar ]
then
    rm $dist_jar
fi

if [ -f $allinone_dist_jar ]
then
    rm $allinone_dist_jar
fi

$build_script
