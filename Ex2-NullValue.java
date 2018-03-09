package org.apache.hadoop.examples;
 
import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class WordCount {
	public static class DescendingComparator extends WritableComparator {
		/***
		 * this is to write a comparator for the key rather than use the defualt increasingly-ordered mode
		 * as the input key format is set as Text, the key would be compared as Text(String), 
		 * for example:
		 * 1, 2, 22, 4, 48
		 * to get a numerical order, keys need to be parsed into number, and then *(-1) to reverse the ordering to decreasing
		 ***/
		public DescendingComparator() {
		   super(Text.class, true);
	   }
	 
		@SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        Text key1 = (Text) w1;
	        Text key2 = (Text) w2;
	        IntWritable realkey1 = new IntWritable(Integer.parseInt(key1.toString()));
	        IntWritable realkey2 = new IntWritable(Integer.parseInt(key2.toString()));
	        return -1 * realkey1.compareTo(realkey2);
	    }
	}
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
 
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
 
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String currentToken = "";
      while (itr.hasMoreTokens()) {
    	  currentToken = itr.nextToken();    
    	//extra conditions can be applied around here, to pick out the key item you would like or shape the key as you wish
    	  word.set(currentToken);
    	  context.write(word, one);
      }
    }
  }
 
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text, IntWritable> {
    private IntWritable result = new IntWritable();
    //private NullWritable nw = NullWritable.get();
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
        
      }  
      result.set(sum);
      
      context.write(key, result);
    }
  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
  //  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    String[] otherArgs =new String[]{"input","output"};
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    //@SuppressWarnings("deprecation")
	Job job = new Job(conf, "word count");
	job.setSortComparatorClass(DescendingComparator.class);// set the comparator to the alternative one written above
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}