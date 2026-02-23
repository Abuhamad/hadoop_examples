import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A MapReduce job to compute the mean temperature per month.
 * - Uses custom WritableComparable (SumCount) for efficient network serialization.
 * - Uses in-mapper combining (cleanup method) to reduce network traffic.
 */
public class Mean {

    /**
     * Custom Writable to store Sum and Count pairs.
     * Must implement WritableComparable to be passed between Map and Reduce phases over the network.
     *
     */
    public static class SumCount implements WritableComparable<SumCount> {

        DoubleWritable sum;
        IntWritable count;

        // Default constructor required by Hadoop for deserialization
        public SumCount() {
            set(new DoubleWritable(0), new IntWritable(0));
        }

        public SumCount(Double sum, Integer count) {
            set(new DoubleWritable(sum), new IntWritable(count));
        }

        public void set(DoubleWritable sum, IntWritable count) {
            this.sum = sum;
            this.count = count;
        }

        public DoubleWritable getSum() {
            return sum;
        }

        public IntWritable getCount() {
            return count;
        }

        public void addSumCount(SumCount sumCount) {
            set(new DoubleWritable(this.sum.get() + sumCount.getSum().get()), 
                new IntWritable(this.count.get() + sumCount.getCount().get()));
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            sum.write(dataOutput);
            count.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            sum.readFields(dataInput);
            count.readFields(dataInput);
        }

        @Override
        public int compareTo(SumCount sumCount) {
            int comparison = sum.compareTo(sumCount.sum);
            if (comparison != 0) {
                return comparison;
            }
            return count.compareTo(sumCount.count);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SumCount sumCount = (SumCount) o;
            return count.equals(sumCount.count) && sum.equals(sumCount.sum);
        }

        @Override
        public int hashCode() {
            int result = sum.hashCode();
            result = 31 * result + count.hashCode();
            return result;
        }
    }

    /**
     * Mapper Class
     * Reads text input, parses Date/MaxTemp, buffers data in memory, and emits SumCount in cleanup.
     *
     */
    public static class MeanMapper extends Mapper<Object, Text, Text, SumCount> {

        private final int DATE = 0;
        // private final int MIN = 1; // Unused in logic
        private final int MAX = 2;

        private Map<Text, List<Double>> maxMap = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            if (values.length != 3) {
                return;
            }

            // Parse date (ddMMyyyy) -> MonthYear (MMyyyy)
            String date = values[DATE];
            Text month = new Text(date.substring(2));
            Double max = Double.parseDouble(values[MAX]);

            if (!maxMap.containsKey(month)) {
                maxMap.put(month, new ArrayList<Double>());
            }

            maxMap.get(month).add(max);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text month : maxMap.keySet()) {
                List<Double> temperatures = maxMap.get(month);
                Double sum = 0d;
                for (Double max : temperatures) {
                    sum += max;
                }
                context.write(month, new SumCount(sum, temperatures.size()));
            }
        }
    }

    /**
     * Reducer Class
     * Aggregates SumCounts and emits final Mean.
     *
     */
    public static class MeanReducer extends Reducer<Text, SumCount, Text, DoubleWritable> {

        private Map<Text, SumCount> sumCountMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            SumCount totalSumCount = new SumCount();

            for (SumCount sumCount : values) {
                totalSumCount.addSumCount(sumCount);
            }

            // Important: We must create a new Text key and new SumCount value to store in map
            // because Hadoop reuses objects during iteration.
            sumCountMap.put(new Text(key), totalSumCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text month : sumCountMap.keySet()) {
                double sum = sumCountMap.get(month).getSum().get();
                int count = sumCountMap.get(month).getCount().get();
                context.write(month, new DoubleWritable(sum / count));
            }
        }
    }

    /**
     * Main Driver
     * Configures the Hadoop Job.
     *
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: Mean <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Mean");
        job.setJarByClass(Mean.class);
        
        // Setup Map and Reduce classes
        job.setMapperClass(MeanMapper.class);
        job.setReducerClass(MeanReducer.class);

        // Setup Output Key/Value types
        // The Mapper outputs Text as Key and SumCount as Value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);

        // The Reducer outputs Text as Key and DoubleWritable as Value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}