package edu.stanford.cs246.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Inverted_Index_NB extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Inverted_Index_NB(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), "Inverted_Index_NB");
      job.setJarByClass(Inverted_Index_NB.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      job.setMapperClass(Map.class);
      job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text();
      private Text filename = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  Scanner s = new Scanner(new File("/home/cloudera/workspace/stopwords.txt"));
    	  ArrayList<String> list = new ArrayList<String>();
    	  while (s.hasNext()){
    	      list.add(s.next());
    	  }
    	  s.close();
    	  
    	  String filenameStr = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			filename = new Text(filenameStr);
			
    	  StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'#--()_\"*/0123456789$%&<>+=@"); 
    	  while (tokenizer.hasMoreTokens()) { 
    		  String stri = tokenizer.nextToken().toLowerCase();
    			if (list.contains(stri)) {
    				
    			} else {
    		  word.set(stri); 
    			context.write(word, filename); 
    			}
         }
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> sum = new ArrayList<String>();

			for (Text value : values) {
					sum.add(value.toString());
				
			}
			HashSet<String>output = new HashSet<String>(sum);

			StringBuilder Stringbuilder = new StringBuilder();
			boolean isfirst = true;
			for (String value : output) {
				if (!isfirst){
				Stringbuilder.append(", ");
				}
				isfirst = false;
				Stringbuilder.append(value+"#"+Collections.frequency(sum, value));
			}

			context.write(key, new Text(Stringbuilder.toString()));

		}
	}
}
