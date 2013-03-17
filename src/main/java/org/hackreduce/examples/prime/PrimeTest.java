package org.hackreduce.examples.prime;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.PrimeMapper;
import org.hackreduce.models.PrimeRecord;


/**
 * This MapReduce job will read the prime dataset and evaluate if the number is prime or not
 */
public class PrimeTest extends Configured implements Tool {

	public static class PrimeTestMapper extends PrimeMapper<Text, LongWritable> {

		@Override
		protected void map(PrimeRecord record, Context context) throws IOException, InterruptedException {
			boolean flag = true;
			for(int i=2; i<=Math.sqrt(record.getNum()); i++) {
	              if (record.getNum()%i == 0) {
	            	  flag = false;
	            	  break;	            	 
	              }
	        }
			if (flag) {
				context.write(new Text(Long.toString(record.getNum())), new LongWritable(1));
			} else {
				// Do nothing.  Number is not prime.  Boo
			}
			
			
		}

	}

	public static class PrimeTestReducer extends Reducer<Text, LongWritable, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text("yes"));
		}

	}

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(PrimeTestMapper.class);
		job.setReducerClass(PrimeTestReducer.class);

		// Configure the job to accept the number list data as input
		PrimeMapper.configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PrimeTest(), args);
		System.exit(result);
	}

}
