package com.skangyam.hadoop.mapreduce.TableJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This Mapper will be specific to EMP table
 * Here the deptid is 3rd col..args[2] as Key
 */
public class MapperEmp extends Mapper<LongWritable, Text, IntWritable, Text> 
{
    protected void map(LongWritable key, Text empD, Context context)
              throws IOException, InterruptedException
    {
        String[] str = empD.toString().split(",");
        int mapK = Integer.parseInt(str[2]);
        context.write(new IntWritable(mapK), new Text(str[0]+","+str[1]));
    }
}
