package com.yxl.itemcf.s1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Step1 reducer 合并同一用户下的所有物品评分
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 下午11:45
 */
public class S1_Reducer extends Reducer<IntWritable,Text,IntWritable,Text>{

    private final static IntWritable k = new IntWritable();
    private final static Text v = new Text();


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        Iterator<Text> it = values.iterator();
        while (it.hasNext()){
            sb.append("," + it.next());
        }

        v.set(sb.toString().replaceFirst(",",""));
        context.write(key,v);
    }
}
