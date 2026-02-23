import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SortWeather {

    /**
     * Mapper: Swaps the data.
     * Input:  "012014   Min: -2.1, Max: 5.0, Mean: 1.45, StdDev: 2.5"
     * Output: Key = 1.45 (Mean), Value = "012014..."
     */
    public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // The input format is: "MonthKey \t StatsString"
            // Example: "012014 \t Min: -5.0, Max: 10.0, Mean: 2.5, StdDev: 1.0"
            
            // 1. Split Key and Value
            String[] parts = line.split("\t");
            if (parts.length < 2) return;

            String stats = parts[1];

            // 2. Extract the Mean value
            // We look for the substring "Mean: " and parse the number after it.
            try {
                String label = "Mean: ";
                int startIndex = stats.indexOf(label);
                if (startIndex != -1) {
                    // Find where the number ends (it might be followed by comma or end of line)
                    int numStart = startIndex + label.length();
                    int numEnd = stats.indexOf(",", numStart);
                    
                    String meanStr;
                    if (numEnd == -1) {
                        meanStr = stats.substring(numStart).trim();
                    } else {
                        meanStr = stats.substring(numStart, numEnd).trim();
                    }

                    double mean = Double.parseDouble(meanStr);
                    
                    // Emit Key: Mean, Value: Original Line
                    context.write(new DoubleWritable(mean), value);
                }
            } catch (Exception e) {
                // If parsing fails, skip line
            }
        }
    }

    /**
     * Comparator: Controls the Sort Order.
     * Standard DoubleWritable sorts Ascending (Smallest -> Largest).
     * We want Descending (Hottest -> Coldest).
     */
    public static class DescendingDoubleComparator extends WritableComparator {
        protected DescendingDoubleComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable k1 = (DoubleWritable) w1;
            DoubleWritable k2 = (DoubleWritable) w2;
            
            // Standard compare is k1.compareTo(k2)
            // Multiply by -1 to reverse order
            return -1 * k1.compareTo(k2);
        }
    }

    /**
     * Reducer: Just writes the lines out.
     * Since the keys arrive sorted, the output file will be sorted.
     */
    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text line : values) {
                // We emit empty key to avoid printing the Mean twice
                context.write(line, new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: SortWeatherOutput <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Sort Weather Output");
        job.setJarByClass(SortWeather.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        
        // SET THE CUSTOM COMPARATOR HERE
        job.setSortComparatorClass(DescendingDoubleComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Ensure we only have 1 Reducer if we want a TOTAL Global Sort.
        // If we have multiple reducers, each file will be sorted locally, 
        // but file-to-file won't be sorted.
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}