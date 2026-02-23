This folder contains Hadoop MapReduce jobs that compute the most frequent words
from text data. `TopNv1` performs a full WordCount and then sorts all terms in
memory (good for learning, not for large data). `TopNv2` and `DynamicTopN` read
WordCount output and keep only the Top N in memory, which scales better.

Compile and Run Java MapReduce Code
===================================

1) Write Your MapReduce Code
----------------------------
Save your MapReduce program in a Java file. This folder includes:

- TopN.java -> class `TopNv1` (single-job WordCount + naive top 20)
- TopNv2.java -> class `TopNv2` (reads WordCount output, top 20)
- TopNv3Dynamic.java -> class `DynamicTopN` (reads WordCount output, top N via -n)

2) Compile the Java Code
------------------------
Use javac and include the Hadoop classpath:

	javac -classpath "$(hadoop classpath)" -d . TopN.java 

- `-classpath`: Adds Hadoop libraries to the compiler classpath.
- `-d .`: Writes the compiled .class files to the current directory.


3) Package the Compiled Classes into a JAR
-----------------------------------------
Create a JAR file from the compiled classes:

	jar -cvf TopN.jar -C . .

- `-cvf`: Creates a JAR file named TopN.jar.
- `-C . .`: Includes all compiled classes in the current directory.


4) Run the MapReduce Jobs
-------------------------
`TopNv1` (one job, does WordCount and top 20 in the reducer):

	hadoop jar TopN.jar TopNv1 /input /topn_v1_out

`TopNv2` (expects WordCount output as input):

	hadoop jar TopN.jar TopNv2 /wordcount_out /topn_v2_out

`DynamicTopN` (expects WordCount output, choose N with -n):

	hadoop jar TopN.jar DynamicTopN -n=50 /wordcount_out /topn_v3_out

**Notes:**

- Use a WordCount job first to create `/wordcount_out` for `TopNv2` and `DynamicTopN`.
- Output directories must not exist before running.


5) Check the Output
-------------------
View the job output:

	hadoop fs -cat /topn_v1_out/part-r-00000 | head
	
    hadoop fs -cat /topn_v2_out/part-r-00000 | head
	
    hadoop fs -cat /topn_v3_out/part-r-00000 | head
