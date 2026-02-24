# WordCount MapReduce
This folder contains Hadoop MapReduce examples for processing text data. The
primary program is a WordCount job that reads lines of text, emits each word as
a key with count 1, and then reduces those counts to totals per word. The steps
below show how to compile the code, package it as a JAR, and run the job on
Hadoop.

Compile and Run Java MapReduce Code
===================================

1) Write Your MapReduce Code
----------------------------
Save your MapReduce program in a Java file, for example WordCountV1.java.

This folder have 
- `WordCounV1.java` --> simple word count program
- `WordCountV2.java` --> you can use arguments like `--skip` and `--wordcount.case.sensitive` 
- `WordCountV3.java` --> you can use arguments like `--wordcount.case.sensitive` and remove punctuations.

2) Compile the Java Code
------------------------
Use javac and include the Hadoop classpath:

	javac -classpath "$(hadoop classpath)" -d . WordCountV1.java

- `-classpath`: Adds Hadoop libraries to the compiler classpath.
- `-d .`: Writes the compiled .class files to the current directory.

3) Package the Compiled Classes into a JAR
-----------------------------------------
Create a JAR file from the compiled classes:

	jar -cvf WordCount.jar -C . .

- `-cvf`: Creates a JAR file named WordCount.jar.
- `-C . .`: Includes all compiled classes in the current directory.

4) Run the MapReduce Job
------------------------
Submit the job to Hadoop:

	hadoop jar WordCount.jar WordCountV1 /input /output

Parameters:

- `WordCount.jar`: JAR file containing your MapReduce program.
- `WordCountV1`: Main class name.
- `/input`: Input directory on HDFS.
- `/output`: Output directory on HDFS (must not exist before running).


Running `WordCountV2` with arguments:

	hadoop jar WordCount.jar WordCountV2 -Dwordcount.case.sensitive=false /input /output -skip /input/stopwords.txt


Running `WordCountV3` with arguments:

	hadoop jar WordCount.jar WordCountV3 -Dwordcount.case.sensitive=false /input /output /input/stopwords.txt

- `WordCount.jar`: JAR file containing your MapReduce program.
- `WordCountV3`: Main class name.
- `/input`: Input directory (or specific file) on HDFS.
- `/output`: Output directory on HDFS (must not exist before running).
- `/input/stopwords.txt`: Skip patterns file on HDFS (must be on HDFS).



5) Check the Output
-------------------
View the job output:

	hadoop fs -cat /output/part-r-00000
