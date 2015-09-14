package com.yxl.itemcf.s2;

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
 * 基于用户的评分矩阵,建立所有物品同现矩阵，以获取相关度
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-09 下午10:43
 */
public class Step2 extends Configured implements Tool {


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

        job.setJobName("itemCF-itemCooc-Matrix-yxl");
        job.setJarByClass(getClass());

        job.setMapperClass(S2_Mapper.class);
        job.setCombinerClass(S2_Reducer.class);
        job.setReducerClass(S2_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1); // 数据量小 设置一个 reduce 方便查看

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true)?0:1;
    }

    // 结果

//    101:101 5
//    101:102 3
//    101:103 4
//    101:104 4
//    101:105 2
//    101:106 2
//    101:107 1
//    102:101 3
//    102:102 3
//    102:103 3
//    102:104 2
//    102:105 1
//    102:106 1
//    103:101 4
//    103:102 3
//    103:103 4
//    103:104 3
//    103:105 1
//    103:106 2
//    104:101 4
//    104:102 2
//    104:103 3
//    104:104 4
//    104:105 2
//    104:106 2
//    104:107 1
//    105:101 2
//    105:102 1
//    105:103 1
//    105:104 2
//    105:105 2
//    105:106 1
//    105:107 1
//    106:101 2
//    106:102 1
//    106:103 2
//    106:104 2
//    106:105 1
//    106:106 2
//    107:101 1
//    107:104 1
//    107:105 1
//    107:107 1


}
