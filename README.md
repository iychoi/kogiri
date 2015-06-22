# kogiri
Massive Comparative Analytic Toolkit for Metagenomics

Build
-----
This project uses "ant" build system. Therefore, user should install proper version of "java" and "ant" to build the project.

Requirements: 
- JDK 1.6
- ant

To build this project, just type below at the root directory of this source package.
```
ant
```

To build all-in-one package of this project including all necessary dependencies, type below.
```
ant allinone
```
or
```
cd scripts
./package_allinone.sh
```

Created jar packages will be located in "/dist" directory.

Preprocess
----------
Kogiri preprocesses FASTA sample files to find matching k-mers efficiently. To run preprocessor, type below.
```
hadoop jar dist/kogiri-all.jar preprocess -k <mer_size> -o <preprocess output dir> <input fasta file dir or files>
```

All vs. All Read Frequency Count
--------------------------------
To count read frequency in all vs. all way, type below.
```
hadoop jar dist/kogiri-all.jar readfrequencycount --histogram <preprocess histogram dir> --statistics <preprocess statistics dir> --stddev_factor 2 -o <output dir> <preprocess kmer index dir or files>
```

License
-------
This project binary and source code is publically accessible under GPL v2 license agreement. If you modify source code and want to distribute, folk this project so that I can keep track of the modifications.

This project includes packages using Apache public license v2. Their license files are located in "/lib" directory.
