package com.yxl.itemcf.s2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Step2 Reducer shuffle 后根据key做和
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:11
 */
public class S2_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            sum += it.next().get();
        }
        result.set(sum);
        context.write(key,result);
    }

}
