package pagerank;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Write the map and reduce functions for each job. For the second job write also the reduce function for the combiner.
 * To test your code run PublicTests.java.
 * On the site submit a zip archive of your src folder.
 * Try also the release tests after your submission. You have 3 trials per hour for the release tests. 
 * A correct implementation will get the same number of points for both public and release tests.
 * Please take the time also to understand the settings for a job, in the next lab your will need to configure it by yourself. 
 */

public class MatrixVectorMult {

	static class FirstMap extends Mapper<LongWritable, Text, IntWritable, Text> {

		IntWritable first_index = new IntWritable();
		IntWritable second_index = new IntWritable();
		DoubleWritable third = new DoubleWritable();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				first_index.set(Integer.parseInt(itr.nextToken())); // is column if vector but is row if matrix
				String s = itr.nextToken(); 
				try
			      {
					second_index.set(Integer.parseInt(s)); // if it can be parsed as int then it was a matrix
					third.set(Double.parseDouble(itr.nextToken()));
					Text result = new Text(first_index.get() + " " + third.get());
					context.write(second_index, result);
			      }
			      catch (NumberFormatException ex)
			      {
			        // then it was a vector
			    	third.set(Double.parseDouble(s));
			    	Text result = new Text(String.valueOf(third.get()));
			    	context.write(first_index, result);
			      }
			}
		}
	}


	static class FirstReduce extends Reducer<IntWritable, Text, NullWritable, Text> {
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			ArrayList<Double> A_col_j = new ArrayList<Double>();
			ArrayList<IntWritable> row_i = new ArrayList<IntWritable>();
			DoubleWritable V_j = new DoubleWritable();
			
			for(Text val : values){// val has once one token and the rest  2 tokens
				String[] r = val.toString().split(" ");
				int counter =r.length;
				
				if(counter==1) {//then it s the vector
					V_j.set(Double.parseDouble(r[0]));
				}else if(counter==2){// then it s the matrix
					row_i.add(new IntWritable(Integer.parseInt(r[0])));
					A_col_j.add(Double.parseDouble(r[1]));
				}
				
			}
			
			for (int i=0;i<row_i.size();i++){
				
				Text result = new Text(row_i.get(i)+" "+key.get()+" "+A_col_j.get(i)*V_j.get());//B_col_j.get(i));
				context.write(null, result);
			}
		}
	}
	
	
	static class SecondMap extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
			{
				//implement here
			IntWritable first_index = new IntWritable();
			IntWritable second_index = new IntWritable();
			DoubleWritable third = new DoubleWritable();
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				first_index.set(Integer.parseInt(itr.nextToken())); // is row of matrix
				second_index.set(Integer.parseInt(itr.nextToken())); // is column
				third.set(Double.parseDouble(itr.nextToken()));
				
				DoubleWritable result = new DoubleWritable(third.get());
				context.write(first_index,result);//key is the row
			
			}
		}
	}

	static class CombinerForSecondMap extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double R_i=0;
			for(DoubleWritable val : values){
				R_i += val.get();
			}
			context.write(key,new DoubleWritable(R_i));
		}
	}

	static class SecondReduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double R_i=0;
			for(DoubleWritable val : values){
				R_i += val.get();
			}
			context.write(key,new DoubleWritable(R_i));

		}
	}

	public static void job(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		// First job
		Job job1 = Job.getInstance(conf);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setMapperClass(FirstMap.class);
		job1.setReducerClass(FirstReduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path[]{new Path(conf.get("initialVectorPath")), new Path(conf.get("inputMatrixPath"))});
		FileOutputFormat.setOutputPath(job1, new Path(conf.get("intermediaryResultPath")));

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);

		job2.setMapperClass(SecondMap.class);
		job2.setReducerClass(SecondReduce.class);

		/* If your implementation of the combiner passed the unit test, uncomment the following line*/
		//job2.setCombinerClass(CombinerForSecondMap.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(conf.get("intermediaryResultPath")));
		FileOutputFormat.setOutputPath(job2, new Path(conf.get("currentVectorPath")));

		job2.waitForCompletion(true);

		FileUtils.deleteQuietly(new File(conf.get("intermediaryResultPath")));
	}

}

