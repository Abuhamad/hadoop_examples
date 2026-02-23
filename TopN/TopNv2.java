import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNv2 {

    /**
     * Mapper: Reads Word Count output. 
     * Keeps a Local Top 20 in memory using a TreeMap.
     */
    public static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {
        
        // TreeMap sorts automatically by its key. We use Count as Key, Word as Value.
        private TreeMap<Integer, String> localTop20 = new TreeMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input is expected to be: "word \t count"
            String[] tokens = value.toString().split("\\t");
            if (tokens.length != 2) return;

            String word = tokens[0];
            int count = Integer.parseInt(tokens[1]);

            // Add to our local Top 20 map
            localTop20.put(count, word);

            // If we have more than 20 items, remove the one with the lowest count
            // firstKey() gets the smallest integer because TreeMap sorts ascending.
            if (localTop20.size() > 20) {
                localTop20.remove(localTop20.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit only the local top 20 to the Reducer.
            // We use NullWritable as key so everything goes to the same reducer.
            // We serialize the state into a single Text string for easy passing.
            for (Map.Entry<Integer, String> entry : localTop20.entrySet()) {
                String payload = entry.getValue() + "\t" + entry.getKey();
                context.write(NullWritable.get(), new Text(payload));
            }
        }
    }

    /**
     * Reducer: Receives the Local Top 20 from all Mappers.
     * Computes the Global Top 20.
     */
    public static class TopNReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

        private TreeMap<Integer, String> globalTop20 = new TreeMap<>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            for (Text val : values) {
                String[] tokens = val.toString().split("\\t");
                String word = tokens[0];
                int count = Integer.parseInt(tokens[1]);

                globalTop20.put(count, word);

                // Keep only the top 20
                if (globalTop20.size() > 20) {
                    globalTop20.remove(globalTop20.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the final Top 20 in descending order (highest counts first)
            for (Map.Entry<Integer, String> entry : globalTop20.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopNv2 <in_from_wordcount> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TopN v2");
        job.setJarByClass(TopNv2.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        // Mappers output NullWritable and Text
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Reducers output Text and IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // VITAL: Force exactly 1 reducer to calculate the GLOBAL Top N
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}