# kogiri
Massive Comparative Analytic Toolkit for Metagenomics


Notice
------
This project is from PKM(https://github.com/iychoi/MR-PKM) project. This project is identical to PKM but under heavy refactoring to provide a better user interface and reusable components for extensions.  

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

License
-------
This project binary and source code is publically accessible under GPL v2 license agreement. If you modify source code and want to distribute, folk this project so that I can keep track of the modifications.
