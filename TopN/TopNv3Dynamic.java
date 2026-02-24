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

public class TopNv3Dynamic {

    /**
     * Mapper: Reads Word Count output. 
     * Keeps a Local Top N in memory using a TreeMap.
     */
    public static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {
        
        private TreeMap<Integer, String> localTopN = new TreeMap<>();
        private int n;

        // The setup method runs ONCE before any map() calls. 
        // We use it to extract our custom 'n' variable from the Configuration.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // Default to 20 if the parameter wasn't provided
            this.n = conf.getInt("top.n.value", 20);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\t");
            if (tokens.length != 2) return;

            String word = tokens[0];
            int count = Integer.parseInt(tokens[1]);

            localTopN.put(count, word);

            // Dynamically bound the map size to N
            if (localTopN.size() > n) {
                localTopN.remove(localTopN.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : localTopN.entrySet()) {
                String payload = entry.getValue() + "\t" + entry.getKey();
                context.write(NullWritable.get(), new Text(payload));
            }
        }
    }

    /**
     * Reducer: Receives the Local Top N from all Mappers.
     * Computes the Global Top N.
     */
    public static class TopNReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

        private TreeMap<Integer, String> globalTopN = new TreeMap<>();
        private int n;

        // Again, use setup to read the parameter from the Context
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.n = conf.getInt("top.n.value", 20);
        }

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] tokens = val.toString().split("\\t");
                String word = tokens[0];
                int count = Integer.parseInt(tokens[1]);

                globalTopN.put(count, word);

                // Dynamically bound the map size to N
                if (globalTopN.size() > n) {
                    globalTopN.remove(globalTopN.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : globalTopN.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        // 1. Parse custom arguments
        int nValue = 20; // Default
        String inputPath = null;
        String outputPath = null;

        for (String arg : otherArgs) {
            if (arg.startsWith("-n=")) {
                try {
                    nValue = Integer.parseInt(arg.substring(3));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number for -n. Using default (20).");
                }
            } else if (inputPath == null) {
                inputPath = arg;
            } else if (outputPath == null) {
                outputPath = arg;
            }
        }

        if (inputPath == null || outputPath == null) {
            System.err.println("Usage: TopNv3Dynamic [-n=<number>] <in_from_wordcount> <out>");
            System.exit(2);
        }

        // 2. Set the custom variable into the configuration BEFORE creating the Job
        conf.setInt("top.n.value", nValue);

        Job job = Job.getInstance(conf, "Dynamic Top N");
        job.setJarByClass(TopNv3Dynamic.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}