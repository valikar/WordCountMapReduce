import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
    public static class Map extends Mapper<LongWritable, Text, Text, Text > {
        private Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new Text("1"));
           }
        }
    }



    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable value = new IntWritable(0);
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                int sum = 0;
                for (Text value : values)
                    sum++;
                    //sum += Integer.parseInt(value.toString());
                value.set(sum);
                context.write(key, value);
            }
    }
    public static class CaderPartitioner extends Partitioner<  Text, Text >
    {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks)

        {


            if(numReduceTasks == 0)
            {
                return 0;
            }

            if(key.toString().matches("^(?i:[abcdefghijklm]).*"))
            {
                return 0;
            }
            else
            {
                return 1 % numReduceTasks;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File("outFiles"));
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        job.setNumReduceTasks(2);
           // set the number of reduce tasks required
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(CaderPartitioner.class);

        FileInputFormat .setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}