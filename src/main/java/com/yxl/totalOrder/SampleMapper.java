package com.yxl.totalOrder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper输出 跟输入一样的值
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-29 下午4:02
 */
public class SampleMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,value);
    }
}
