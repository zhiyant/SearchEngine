#!/usr/bin/sh

hdfs dfs -rm -r  /final/SearchEngine/index /final/SearchEngine/tmp /final/SearchEngine/secondary_index
hadoop jar final.jar /final/SearchEngine/web /final/SearchEngine/index /final/SearchEngine/secondary_index