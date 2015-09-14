package com.yxl.itemcf.s5;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Step5 Mapper 原样输出
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:19
 */
public class S5_Mapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = Main.DELIMITER.split(value.toString());
        Text k = new Text(tokens[0]);
        Text v = new Text(tokens[1]+","+tokens[2]);
        context.write(k, v);
    }

}
