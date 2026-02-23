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

import java.io.IOException;
import java.util.*;

public class TopNv1 {

    /**
     * The Mapper reads one line at a time, cleans it, splits it into words,
     * and emits every word with a value of 1.
     * (This is the standard, unoptimized WordCount Mapper).
     */
    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private final String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
            StringTokenizer itr = new StringTokenizer(cleanLine);
            
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().trim());
                context.write(word, one);
            }
        }
    }

    /**
     * The Naive Reducer retrieves every word, sums its counts, and puts EVERY
     * unique word into a HashMap. In the cleanup phase, it sorts the entire map
     * and emits the Top 20.
     */
    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // FATAL FLAW FOR BIG DATA: This map grows infinitely with the number of unique words.
        private Map<String, Integer> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Storing every single word and its total count in JVM memory
            countMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the massive map by values (counts) in descending order
            Map<String, Integer> sortedMap = sortByValuesDescending(countMap);

            int counter = 0;
            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                if (counter++ >= 20) {
                    break;
                }
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }

        /**
         * Helper method to sort a Map by its values in descending order.
         * Doing this on a map with millions of entries will cause extreme CPU/Memory spikes.
         */
        private Map<String, Integer> sortByValuesDescending(Map<String, Integer> unsortedMap) {
            List<Map.Entry<String, Integer>> list = new LinkedList<>(unsortedMap.entrySet());

            // Sort list with a custom comparator (Descending)
            list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

            // Put sorted data into a LinkedHashMap to preserve the insertion order
            Map<String, Integer> sortedMap = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : list) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }
            return sortedMap;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopN v1 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TopN v1");
        job.setJarByClass(TopNv1.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        
        // Output types for Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Output types for Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Force exactly 1 reducer so we get a single global Top 20 list.
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}