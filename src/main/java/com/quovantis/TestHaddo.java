package com.quovantis;

import java.io.IOException;
import java.net.URI;
import java.util.Dictionary;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestHaddo {
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private Text word = new Text();
        private static LongWritable hits = new LongWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	String[] content = value.toString().split(" ");
        	word.set(content[0]);
        	hits.set(Long.parseLong(content[2]));
            context.write(word, hits);
           
        }
    }
	
	public static class AllTranslationsReducer extends Reducer<Text,LongWritable,Text,LongWritable>
    {
        private Text result = new Text();
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            long sum = 0;
            for (LongWritable val : values)
            {
                sum += val.get();
            }
            //result.set(translations);
            context.write(key, new LongWritable(sum));
        }
    }
	
	public static void main(String[] args) throws Exception
    {
		Configuration conf = new Configuration();
        Job job = new Job(conf, "dictionary");
        job.setJarByClass(TestHaddo.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(AllTranslationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/In/pagecounts-20160404-110000"));
     // configuration should contain reference to your namenode
        FileSystem fs = FileSystem.get(conf);
        // true stands for recursively deleting the folder you gave
        fs.delete(new Path("/output/wikipedia"), true);
        FileOutputFormat.setOutputPath(job, new Path("/output/wikipedia"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
