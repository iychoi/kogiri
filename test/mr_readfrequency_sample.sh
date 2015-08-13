#!/bin/bash

# run preprocess

dist_dir="dist"
allinone_dist_jar="$dist_dir/kogiri-all.jar"
build_script="scripts/package_allinone.sh"

preprocess_path="test/mr_preprocess"
input_path="$preprocess_path/kmerindex"
histogram_path="$preprocess_path/histogram"
statistics_path="$preprocess_path/statistics"
output_path="test/mr_readfrequency"

change_dir() {
    if [[ $PWD == *"/test" ]]
    then
    	cd ..
	fi
}

# move to right directory
change_dir

if [ ! -f $allinone_dist_jar ]
then
    echo "package file not found! - $allinone_dist_jar"
    
    # build jar first
    $build_script
fi

if [ $? == 0 ]
then
    echo "run kogiri..."
    hadoop jar $allinone_dist_jar readfrequencycount $@ --histogram $histogram_path --statistics $statistics_path --stddev_factor 2 -o $output_path $input_path
fi

