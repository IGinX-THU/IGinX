#!/bin/bash

if [ "$RUNNER_OS" = "Linux" ]; then


elif [ "$RUNNER_OS" = "Windows" ]; then
  echo "windows"

elif [ "$RUNNER_OS" = "macOS" ]; then
  # 根据 https://blog.csdn.net/mei86233824/article/details/81066999 修改makefile文件并进行编译生成可执行文件
  cp makefile.suite makefile
  awk 'NR < 103 || NR > 111 { print } NR == 103 { print "CC      = gcc\n# Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)\n#                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE\n# Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, \n#                                  SGI, SUN, U2200, VMS, LINUX, WIN32 \n# Current values for WORKLOAD are:  TPCH\nDATABASE= SQLSERVER\nMACHINE = LINUX\nWORKLOAD = TPCH" }' makefile > new_makefile
  mv new_makefile makefile

  sed 's/#include <malloc.h>/#include <sys\/malloc.h>/' bm_utils.c > new_bm_utils.c
  mv new_bm_utils.c bm_utils.c
  sed 's/#include <malloc.h>/#include <sys\/malloc.h>/' varsub.c > new_varsub.c
  mv new_varsub.c varsub.c
fi