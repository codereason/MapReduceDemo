package com.auguigu.mapreduce.index;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行 2截取 3 获取文件名称 4 输出
        String line = value.toString();
        String[] fields = line.split(" ");
        FileSplit inputsplit = (FileSplit) context.getInputSplit();
        String name = inputsplit.getPath().getName();
        for(int i = 0; i < fields.length;i++){
            k.set(fields[i] +"--"+ name);
            context.write(k,new IntWritable(1));
        }



    }
}
