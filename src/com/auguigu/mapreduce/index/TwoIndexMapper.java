package com.auguigu.mapreduce.index;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行 2截取 3 获取文件名称 4 输出
        String line = value.toString();
        String[] fields = line.split("--");
        k.set(fields[0]);
        v.set(fields[1]);
        context.write(k, v);
    }


}

