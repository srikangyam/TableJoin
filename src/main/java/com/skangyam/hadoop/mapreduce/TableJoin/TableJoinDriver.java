package com.skangyam.hadoop.mapreduce.TableJoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TableJoinDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input1 dir> <input2 Directory> <output dir>\n",
					getClass().getSimpleName());
			return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(TableJoinDriver.class);
		job.setJobName(this.getClass().getName());
		
		Path input1 = new Path(args[0]);
		Path input2 = new Path(args[1]);
		
		MultipleInputs.addInputPath(job, input1, TextInputFormat.class, MapperEmp.class);
		MultipleInputs.addInputPath(job, input2, TextInputFormat.class, MapperDept.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(ReducerJoin.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true));
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TableJoinDriver(), args);
		System.exit(exitCode);

	}

}
