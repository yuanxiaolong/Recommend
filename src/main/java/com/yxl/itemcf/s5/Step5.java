package com.yxl.itemcf.s5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Step5 将线性变换后的评分做和，得到推荐矩阵
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 下午6:45
 */
public class Step5 extends Configured implements Tool {

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

        job.setJobName("itemCF-Sum-Matrix-yxl");
        job.setJarByClass(getClass());

        job.setMapperClass(S5_Mapper.class);
        job.setCombinerClass(S5_Reducer.class);
        job.setReducerClass(S5_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // 数据量小 设置一个 reduce 方便查看

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true)?0:1;
    }

// 结果

//    1       107,5.0
//    1       106,18.0
//    1       105,15.5
//    1       104,33.5
//    1       103,39.0
//    1       102,31.5
//    1       101,44.0
//    2       107,4.0
//    2       106,20.5
//    2       105,15.5
//    2       104,36.0
//    2       103,41.5
//    2       102,32.5
//    2       101,45.5
//    3       107,15.5
//    3       106,16.5
//    3       105,26.0
//    3       104,38.0
//    3       103,24.5
//    3       102,18.5
//    3       101,40.0
//    4       107,9.5
//    4       106,33.0
//    4       105,26.0
//    4       104,55.0
//    4       103,53.5
//    4       102,37.0
//    4       101,63.0
//    5       107,11.5
//    5       106,34.5
//    5       105,32.0
//    5       104,59.0
//    5       103,56.5
//    5       102,42.5
//    5       101,68.0


}
