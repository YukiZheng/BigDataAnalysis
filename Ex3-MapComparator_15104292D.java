package org.apache.hadoop.examples;
 
import java.io.IOException;
import java.util.StringTokenizer;


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
 
public class WordCount {
 
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, NullWritable>{
	  //the output value type can be changed to NullWritable (null in mapreduce fashion)
	  //if we don't need an actually value to be output, null can be put in
	  //Attention: once changed here the following code needs to be consistent with this change
 
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
    	  context.write(word, NullWritable.get());
        //NullWritable.get(), gives a null value.
      }
    }
  }
 
  public static class IntSumReducer 
  		//be consistent
       extends Reducer<Text,NullWritable,Text, NullWritable> {
    //private IntWritable result = new IntWritable();
    private NullWritable nw = NullWritable.get();
    public void reduce(Text key, Iterable<NullWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
      /***int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
        
      }  
      result.set(sum);***/
      context.write(key, nw);
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
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);//can be changed as well, if needed
    job.setOutputValueClass(NullWritable.class);//the initiation of the mapreduce job also needs to be consistent by changing to null
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}