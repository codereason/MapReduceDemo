package com.auguigu.mapreduce.index;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text text : values) sb.append(text.toString() + "\t");
        //设置输出value
        v.set(sb.toString());
        context.write(key, v);
    }
}
