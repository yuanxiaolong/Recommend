package com.yxl.itemcf.s2;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Step2 Mapper 2次遍历，为每次出现则设置1次
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:10
 */
public class S2_Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private final static Text k = new Text();
    private final static IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = Main.DELIMITER.split(value.toString());

        for (int i = 1; i < tokens.length; i++) {
            String itemID = tokens[i].split(":")[0];
            for (int j = 1; j < tokens.length; j++) {
                String itemID2 = tokens[j].split(":")[0];
                k.set(itemID + ":" + itemID2);
//                System.out.println("key " + k.toString() + " value " + 1);
                context.write(k, v);
            }
        }

    }

}
