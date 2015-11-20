package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Write the map and reduce function. To test your code run PublicTests.java. 
 * On the site submit a zip archive of your src folder. 
 * Try also the release tests after your submission. You have 3 trials per hour for the release tests. 
 * A correct implementation will get the same number of points for both public and release tests.
 * Please take the time to understand the settings for a job, in the next lab your will need to configure it by yourself. 
 */

public class GraphToMatrix {

	static class Map extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		IntWritable successor = new IntWritable();
		IntWritable webpage = new IntWritable();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				webpage.set(Integer.parseInt(itr.nextToken()));
				successor.set(Integer.parseInt(itr.nextToken()));
				context.write(webpage, successor);
			}
		}

	}

	static class Reduce extends
			Reducer<IntWritable, IntWritable, NullWritable, Text> {

		
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			ArrayList<Integer> successors = new ArrayList<Integer>() ;
			for(IntWritable val : values){
				count +=1;
				successors.add(val.get());
			}
			
			double weight = 1/((double) count);
			
			for (int s : successors){
				Text result = new Text(s+" "+key.get()+" "+weight);
				context.write(null, result);
			}
			
						
		}
	}

	public static void job(Configuration conf) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job,
				new Path(conf.get("processedGraphPath")));
		FileOutputFormat.setOutputPath(job,
				new Path(conf.get("stochasticMatrixPath")));
		job.waitForCompletion(true);
	}

}
