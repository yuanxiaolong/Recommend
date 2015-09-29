package com.yxl.totalOrder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输出
 * author: xiaolong.yuanxl
 * date: 2015-09-29 下午3:09
 */
public class ValueReducer extends Reducer<Text,Text,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t : values){
            context.write(t,NullWritable.get());
        }
    }
}
