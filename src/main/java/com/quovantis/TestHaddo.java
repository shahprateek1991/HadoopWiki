package com.quovantis;
/**
 * Schema of the wikipedia data file
 * |Language|Resource|Hits|Size 
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Dictionary;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestHaddo {
	
	public static class WordMapper extends Mapper<LongWritable, Text, CompositeKey, LongWritable >
    {
        private Text word = new Text();
        private static LongWritable hits = new LongWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	String[] content = value.toString().split(" ");
        	try {
				word.set(content[0]);
				hits.set(Long.parseLong(content[2]));
			} catch (NumberFormatException e) {
				System.out.println("First:"+content[0]+" Second:"+ content[2]);
				System.out.println(key+"  "+value);
			}
        	CompositeKey compositeKey = new CompositeKey(word.toString(), hits.get());
            context.write(compositeKey, hits);
           
        }
    }
	
	public static class AllTranslationsReducer extends Reducer<CompositeKey, LongWritable,Text,LongWritable>
    {
        private Text result = new Text();
        public void reduce(CompositeKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            long sum = 0;
            for (LongWritable val : values)
            {
                sum += val.get();
            }
            //result.set(translations);
            context.write(new Text(key.getText()), new LongWritable(sum));
        }
    }
	
	public static void main(String[] args) throws Exception
    {
		Configuration conf = new Configuration();
        Job job = new Job(conf, "dictionary");
        job.setJarByClass(TestHaddo.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(AllTranslationsReducer.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setGroupingComparatorClass(NaturalKeyComparator.class);
        job.setPartitionerClass(CustomPartitioner.class);
        //job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/home/prateekshah/Downloads/pagecounts-20160404-110000")); // Input file
        FileSystem fs = FileSystem.get(conf);
        // true stands for recursively deleting the folder you gave
        fs.delete(new Path("/home/prateekshah/Downloads/output/wikipedia"), true);
        FileOutputFormat.setOutputPath(job, new Path("/home/prateekshah/Downloads/output/wikipedia")); //Output file
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

	
	public static class CompositeKeyComparator extends WritableComparator {

		protected CompositeKeyComparator() {
			super(CompositeKey.class,true);
			// TODO Auto-generated constructor stub
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return ((CompositeKey)w1).compareTo((CompositeKey)w2);
		}
		
		/*@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			CompositeKey key1 = WritableUtils.
		    return 0;
		}*/

		
	}
	
	public static class NaturalKeyComparator extends WritableComparator {

		protected NaturalKeyComparator() {
			super(CompositeKey.class,true);
			// TODO Auto-generated constructor stub
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return (int)(((CompositeKey)w1).getText().compareTo(((CompositeKey)w2).getText()));
		}
		
		/*@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		    return 0;
		}*/

		
	}
	
	public static class CustomPartitioner extends Partitioner<CompositeKey, LongWritable> {

		@Override
		public int getPartition(CompositeKey arg0, LongWritable arg1, int arg2) {
			// TODO Auto-generated method stub
			return arg0.getText().hashCode() % arg2;
		}
		
	}
}
