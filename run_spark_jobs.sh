#!/bin/bash
echo "Press 1 to count the namber of times each word appears"
echo "Press 2 to filter out all words that show up less than 1 million times"
echo "Press 3 to count only the letter occurs in the list of case 2"
echo ""
read -p "What do you chose? " type

jobName=word

#Get current folder
curent_dir=$(pwd)

jarPath=
classPath=
nameJar=

if [ $jobName == word ]
then
	nameJar=WordCount.jar
	jarPath=$curent_dir/Jars/$nameJar
	classPath=com.myspark.jobs.WordCount
else
	echo "Does not exist jobName " $jobName
fi

#HadoopProject
inputFilePath=$curent_dir/Input/Data

#Hadoop
envPath=/user/cloudera
inputPath=$envPath/input
outputPath=$envPath/output/$jobName

echo "Step 0: Create Jar filename"

if [ ! -f $jarPath ]
then
	echo "Create new jar driver file"
	cd $curent_dir/wordcount/target/classes
	jar -cvfm $nameJar $jobName.MF *
	mv -f $nameJar $curent_dir/Jars
	cd $curent_dir
fi

echo "Step 1: Remove folder of input and output"
hadoop fs -rm -r $inputPath
hadoop fs -rm -r $outputPath

echo "Step 2: Create folder to store input data"
hadoop fs -mkdir $envPath $inputPath 

echo "Step 3: Copy data to input folder"
hadoop fs -put $inputFilePath $inputPath

echo "Step 4: Run spark on Hadoop cluster"
spark-submit --class $classPath --master local $jarPath $inputPath $outputPath $type

echo "Step 5: Copy from Hadoop to local"
if [ -f $curent_dir/Outputs/$jobName ]
then
	rm -r $curent_dir/Outputs/$jobName
fi

hadoop fs -copyToLocal $outputPath $curent_dir/Outputs

echo "Step 6: View all results"
hadoop fs -cat $outputPath/*

read -p "Completed!"

