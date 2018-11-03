package com.auguigu.mapreduce.index;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //累加求和
        int count = 0;
        for (IntWritable value:values){
            count+=value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
