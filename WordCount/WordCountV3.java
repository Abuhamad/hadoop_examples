import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class WordCount3 {

    /**
     * MAPPER CLASS
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text wordOut = new Text();
        private Set<String> patternsToSkip = new HashSet<>();
        
        // Create a variable to hold the flag
        private boolean caseSensitive;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            // 1. Retrieve the flag from the configuration (default is true)
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile.getPath());
                    if (path.getName().equals("pattern.txt")) {
                        loadSkipPatterns(path.getName());
                    }
                }
            }
        }

        private void loadSkipPatterns(String fileName) {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String pattern;
                while ((pattern = br.readLine()) != null) {
                    pattern = pattern.trim();
                    
                    // 2. Only lowercase the skip patterns if we are NOT case-sensitive
                    if (!caseSensitive) {
                        pattern = pattern.toLowerCase();
                    }
                    
                    if (!pattern.isEmpty()) {
                        patternsToSkip.add(pattern);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading pattern.txt: " + e.getMessage());
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 1. PUNCTUATION REMOVAL
            String cleanLine = value.toString().replaceAll("[^a-zA-Z\\s]", "");

            // 3. APPLY CASE SENSITIVITY FLAG
            if (!caseSensitive) {
                cleanLine = cleanLine.toLowerCase();
            }

            StringTokenizer itr = new StringTokenizer(cleanLine);
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();

                if (!patternsToSkip.contains(token)) {
                    wordOut.set(token);
                    context.write(wordOut, one);
                }
            }
        }
    }

    /**
     * REDUCER CLASS
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * MAIN DRIVER
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        
        // We expect: <in> <out> -skip <pattern_file>
        if (remainingArgs.length != 4 || !remainingArgs[2].equals("-skip")) {
            System.err.println("Usage: WordCount <in> <out> -skip <pattern.txt>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount3.class);
        
        job.setMapperClass(TokenizerMapper.class);
        
        // Use the Reducer as a Combiner to optimize network traffic
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add the pattern.txt file to the Distributed Cache
        String patternFilePath = remainingArgs[3];
        job.addCacheFile(new URI(patternFilePath));

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}