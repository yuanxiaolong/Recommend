package com.yxl.itemcf.s3_2;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Step3_2 原样输出
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:15
 */
public class S3_2_Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private final static Text k = new Text();
    private final static IntWritable v = new IntWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = Main.DELIMITER.split(value.toString());
        k.set(tokens[0]);
        v.set(Integer.parseInt(tokens[1]));
        context.write(k, v);    // 原样输出
    }

}
