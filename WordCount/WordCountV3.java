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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class WordCountV3 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordOut = new Text();
        private Set<String> patternsToSkip = new HashSet<>();
        private boolean caseSensitive;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // Read the flag passed via -D command line
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);

            // Access the file using the symlink name "pattern.txt"
            File file = new File("pattern.txt");
            if (file.exists()) {
                loadSkipPatterns("pattern.txt");
            } else {
                System.err.println("CRITICAL: pattern.txt not found in working directory!");
            }
        }

        private void loadSkipPatterns(String fileName) {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String pattern;
                while ((pattern = br.readLine()) != null) {
                    pattern = pattern.trim();
                    if (!caseSensitive) {
                        pattern = pattern.toLowerCase();
                    }
                    if (!pattern.isEmpty()) {
                        patternsToSkip.add(pattern);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading skip file: " + e.getMessage());
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Remove punctuation and convert to String
            String cleanLine = value.toString().replaceAll("[^a-zA-Z\\s]", " ");

            if (!caseSensitive) {
                cleanLine = cleanLine.toLowerCase();
            }

            StringTokenizer itr = new StringTokenizer(cleanLine);
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                // Check against the HashSet loaded in setup()
                if (!patternsToSkip.contains(token)) {
                    wordOut.set(token);
                    context.write(wordOut, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) { sum += val.get(); }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: WordCountV3 <in> <out> <skip_file_path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Word Count v3");
        job.setJarByClass(WordCountV3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // KEY FIX: The #pattern.txt creates a symbolic link in the container's working directory
        job.addCacheFile(new URI(remainingArgs[2] + "#pattern.txt"));

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
