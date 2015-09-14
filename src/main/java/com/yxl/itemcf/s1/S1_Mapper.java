package com.yxl.itemcf.s1;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Step1 Mapper 用户对单个物品评分
 * author: xiaolong.yuanxl
 * date: 2015-09-10 下午11:44
 */
public class S1_Mapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    private final static IntWritable k = new IntWritable();
    private final static Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = Main.DELIMITER.split(value.toString());
        int userID = Integer.parseInt(tokens[0]);
        String itemId = tokens[1];
        String pref = tokens[2];

        k.set(userID);
        v.set(itemId + ":" + pref);

        context.write(k,v);
    }

}
