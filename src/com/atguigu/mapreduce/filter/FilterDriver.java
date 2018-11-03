package com.atguigu.mapreduce.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(FilterDriver.class);
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// Ҫ���Զ���������ʽ������õ�job��
		job.setOutputFormatClass(FilterOutputformat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// ��Ȼ�����Զ�����outputformat��������Ϊ���ǵ�outputformat�̳���fileoutputformat
		// ��fileoutputformatҪ���һ��_SUCCESS�ļ������ԣ����⻹��ָ��һ�����Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
