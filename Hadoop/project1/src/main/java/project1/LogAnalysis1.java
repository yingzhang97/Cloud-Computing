package project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class LogAnalysis1
{
    private static String target = "/assets/img/home-logo.png";

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        public static int n = 0;
        private final static IntWritable one = new IntWritable(1);
        private Text cache = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            try
            {
                String[] components = value.toString().split(" ");
                if (components[6].equals(target))
                {
                    cache.set(components[6]);
                    context.write(cache, one);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LogAnalysis1");
        job.setJarByClass(LogAnalysis1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}