#!/bin/sh

# how to use:
# ./thisfile.sh <nameOfFileToDuplicate> <fileToBeBasedOn>

backUpName="${1}.bak"
cp $1 $backUpName

target="$(wc -l < $2)"

echo "Trying to make $1 to have $target number of lines"

existingLines="$(wc -l < $backUpName)"

echo "$1 has $existingLines existing lines"

lines="$(wc -l < $1)"

while [ $lines -lt $target ]
do
	diff=`expr $target - $lines`
	#echo "diff is $diff"
	if [ $diff -lt $existingLines ]
	then
		#echo "go to if"
		# take only certain lines
		head -$diff $backUpName >> $1
	else
		#echo "go to else"
		cat $backUpName >> $1
	fi
	lines="$(wc -l < $1)"
	#echo "new lines $lines"
done

if [ $lines = $target ]
then
	echo "Task finished. $1 has $lines number of lines now."
else
	echo "Warning: number of lines is still not the same. Please check manually why."
fi
