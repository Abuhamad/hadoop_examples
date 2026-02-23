import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WeatherStats {

    /**
     * Custom Writable to hold stats state: Min, Max, Sum, SumSquares, Count.
     */
    public static class StatsWritable implements Writable {
        private double min;
        private double max;
        private double sum;
        private double sumSquares;
        private int count;

        public StatsWritable() {
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
            this.sum = 0;
            this.sumSquares = 0;
            this.count = 0;
        }

        // Initialize with a single value
        public StatsWritable(double value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.sumSquares = value * value;
            this.count = 1;
        }

        public void merge(StatsWritable other) {
            if (other.count == 0) return;
            
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.sumSquares += other.sumSquares;
            this.count += other.count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(min);
            out.writeDouble(max);
            out.writeDouble(sum);
            out.writeDouble(sumSquares);
            out.writeInt(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            min = in.readDouble();
            max = in.readDouble();
            sum = in.readDouble();
            sumSquares = in.readDouble();
            count = in.readInt();
        }

        @Override
        public String toString() {
            // Calculate final stats for output
            double mean = sum / count;
            
            // Variance = E[X^2] - (E[X])^2
            double variance = (sumSquares / count) - (mean * mean);
            // Handle precision errors resulting in negative variance close to zero
            if (variance < 0) variance = 0;
            double stdDev = Math.sqrt(variance);

            return String.format("Min: %.2f, Max: %.2f, Mean: %.2f, StdDev: %.2f", 
                                 min, max, mean, stdDev);
        }
    }

    /**
     * Mapper: Reads data and performs In-Mapper Combining.
     */
    public static class StatsMapper extends Mapper<Object, Text, Text, StatsWritable> {

        // Index 2 = Max Temp column. Change to 1 for Min Temp column.
        private static final int TARGET_COLUMN = 2; 
        
        // Cache for In-Mapper Combining
        private Map<String, StatsWritable> mapCache;

        @Override
        protected void setup(Context context) {
            mapCache = new HashMap<>();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");

            if (tokens.length != 3) return;

            try {
                // Parse Month-Year Key (e.g., 01012014 -> 012014)
                String date = tokens[0];
                String monthKey = date.substring(2);

                // Parse Value
                double temp = Double.parseDouble(tokens[TARGET_COLUMN]);

                // Update Cache
                StatsWritable stats = mapCache.get(monthKey);
                if (stats == null) {
                    mapCache.put(monthKey, new StatsWritable(temp));
                } else {
                    stats.merge(new StatsWritable(temp));
                }

            } catch (NumberFormatException e) {
                // Ignore bad lines
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Flush cache to Reducer
            for (Map.Entry<String, StatsWritable> entry : mapCache.entrySet()) {
                context.write(new Text(entry.getKey()), entry.getValue());
            }
        }
    }

    /**
     * Reducer: Aggregates partial stats from Mappers.
     */
    public static class StatsReducer extends Reducer<Text, StatsWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
            StatsWritable totalStats = new StatsWritable();

            // Iterate over all partial stats and merge them
            for (StatsWritable val : values) {
                totalStats.merge(val);
            }

            // Write final result
            // The toString() method of StatsWritable formats the output
            context.write(key, new Text(totalStats.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: WeatherStats <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Weather Statistics");
        job.setJarByClass(WeatherStats.class);

        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatsWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}