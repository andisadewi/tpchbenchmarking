#!/bin/bash

# how to use:
# ./thisFile.sh <scaleFactor>

files=(
	"orders.tbl" 
	"customer.tbl" 
	"part.tbl" 
	"partsupp.tbl" 
	"supplier.tbl"
	)

for file in "${files[@]}"
do	
	result="$(./duplicate.sh ${1}/$file ${1}/lineitem.tbl | grep finished)"
	if [ -z "$result" ]
	then
		echo "Duplication on $file failed. Exiting..."
		exit
	else
		echo "$file successfully duplicated."
	fi
done

echo "Task finished."

