package com.yxl.itemcf.s3_1;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Step3_1 Mapper 同一个物品 不同用户的评分
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:14
 */
public class S3_1_Mapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    private final static IntWritable k = new IntWritable();
    private final static Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = Main.DELIMITER.split(value.toString());
        for (int i = 1; i < tokens.length; i++) {
            String[] vector = tokens[i].split(":");
            int itemID = Integer.parseInt(vector[0]);
            String pref = vector[1];

            k.set(itemID);
            v.set(tokens[0] + ":" + pref);
            context.write(k, v);
        }
    }

}
