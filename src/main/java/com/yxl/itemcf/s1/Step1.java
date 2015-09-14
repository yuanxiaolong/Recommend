package com.yxl.itemcf.s1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 建立用户对已有物品的评分矩阵
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-09 下午9:43
 */
public class Step1 extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        String input = strings[0];
        String output = strings[1];

        Configuration conf = new Configuration();

        // 删除已有结果集
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path(output);
        if (fs.exists(out)) {
            fs.delete(out,true);
        }

        Job job = Job.getInstance();

        job.setJobName("itemCF-UserScoreToItem-Matrix-yxl");
        job.setJarByClass(getClass());

        job.setMapperClass(S1_Mapper.class);
        job.setCombinerClass(S1_Reducer.class);
        job.setReducerClass(S1_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // 数据量小 设置一个 reduce 方便查看

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true)?0:1;
    }



//    结果

//    1       102:3.0,103:2.5,101:5.0
//    2       101:2.0,102:2.5,103:5.0,104:2.0
//    3       107:5.0,101:2.0,104:4.0,105:4.5
//    4       101:5.0,103:3.0,104:4.5,106:4.0
//    5       101:4.0,102:3.0,103:2.0,104:4.0,105:3.5,106:4.0

}
