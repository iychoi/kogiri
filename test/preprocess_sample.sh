#!/bin/bash

# run preprocess

dist_dir="dist"
allinone_dist_jar="$dist_dir/kogiri-all.jar"
build_script="script/package_allinone.sh"

mer_size="20"
input_path="test/sample"
output_path="test/preprocess"

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
    # remove old output
    if [ -d $output_path ]
    then
        echo "output directory exists, removing..."
        rm -rf $output_path
    fi
    
    echo "run kogiri..."
    hadoop jar $allinone_dist_jar preprocess -k $mer_size -o $output_path $input_path
fi

