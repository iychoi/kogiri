#! /usr/bin/env python

import shutil
import os
import os.path
import subprocess
import sys
import glob

package_path = "dist/kogiri-all.jar"
mer_size = 20
input_path = "test/sample"
output_path = "test/preprocess"

def main():
    os.chdir("..")
    subprocess.call("hadoop jar " + package_path + " " + 
                        "preprocess " +
                        "-k " + str(mer_size) + " " + 
                        "-o " + output_path + " " +
                        input_path, shell=True)

if __name__ == "__main__":
    main()
