package com.yxl.itemcf.s4;

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
 * 矩阵做乘，线性变换
 *
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 上午9:42
 */
public class Step4 extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        String input_1 = strings[0];
        String input_2 = strings[1];
        String output = strings[2];

        Configuration conf = new Configuration();

        // 删除已有结果集
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path(output);
        if (fs.exists(out)) {
            fs.delete(out,true);
        }

        Job job = Job.getInstance();

        job.setJobName("itemCF-Aggregate-Matrix-yxl");
        job.setJarByClass(getClass());

        job.setMapperClass(S4_Mapper.class);
//        job.setCombinerClass(S4_Reducer.class); // 这里不能用 combiner 如果本地直接先做一次 reduce 则输出为空
        job.setReducerClass(S4_Reducer.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // 数据量小 设置一个 reduce 方便查看


        FileInputFormat.addInputPaths(job, input_1 + "," + input_2);
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true)?0:1;
    }

    //结果
//    3       107,2.0
//    2       107,2.0
//    1       107,5.0
//    5       107,4.0
//    4       107,5.0
//    3       106,4.0
//    2       106,4.0
//    1       106,10.0
//    5       106,8.0
//    4       106,10.0
//    3       105,4.0
//    2       105,4.0
//    1       105,10.0
//    5       105,8.0
//    4       105,10.0
//    3       104,8.0
//    2       104,8.0
//    1       104,20.0
//    5       104,16.0
//    4       104,20.0
//    3       103,8.0
//    2       103,8.0
//    1       103,20.0
//    5       103,16.0
//    4       103,20.0
//    3       102,6.0
//    2       102,6.0
//    1       102,15.0
//    5       102,12.0
//    4       102,15.0
//    3       101,10.0
//    2       101,10.0
//    1       101,25.0
//    5       101,20.0
//    4       101,25.0
//    2       106,2.5
//    1       106,3.0
//    5       106,3.0
//    2       105,2.5
//    1       105,3.0
//    5       105,3.0
//    2       104,5.0
//    1       104,6.0
//    5       104,6.0
//    2       103,7.5
//    1       103,9.0
//    5       103,9.0
//    2       102,7.5
//    1       102,9.0
//    5       102,9.0
//    2       101,7.5
//    1       101,9.0
//    5       101,9.0
//    2       106,10.0
//    1       106,5.0
//    5       106,4.0
//    4       106,6.0
//    2       105,5.0
//    1       105,2.5
//    5       105,2.0
//    4       105,3.0
//    2       104,15.0
//    1       104,7.5
//    5       104,6.0
//    4       104,9.0
//    2       103,20.0
//    1       103,10.0
//    5       103,8.0
//    4       103,12.0
//    2       102,15.0
//    1       102,7.5
//    5       102,6.0
//    4       102,9.0
//    2       101,20.0
//    1       101,10.0
//    5       101,8.0
//    4       101,12.0
//    3       107,4.0
//    2       107,2.0
//    5       107,4.0
//    4       107,4.5
//    3       106,8.0
//    2       106,4.0
//    5       106,8.0
//    4       106,9.0
//    3       105,8.0
//    2       105,4.0
//    5       105,8.0
//    4       105,9.0
//    3       104,16.0
//    2       104,8.0
//    5       104,16.0
//    4       104,18.0
//    3       103,12.0
//    2       103,6.0
//    5       103,12.0
//    4       103,13.5
//    3       102,8.0
//    2       102,4.0
//    5       102,8.0
//    4       102,9.0
//    3       101,16.0
//    2       101,8.0
//    5       101,16.0
//    4       101,18.0
//    3       107,4.5
//    5       107,3.5
//    3       106,4.5
//    5       106,3.5
//    3       105,9.0
//    5       105,7.0
//    3       104,9.0
//    5       104,7.0
//    3       103,4.5
//    5       103,3.5
//    3       102,4.5
//    5       102,3.5
//    3       101,9.0
//    5       101,7.0
//    5       106,8.0
//    4       106,8.0
//    5       105,4.0
//    4       105,4.0
//    5       104,8.0
//    4       104,8.0
//    5       103,8.0
//    4       103,8.0
//    5       102,4.0
//    4       102,4.0
//    5       101,8.0
//    4       101,8.0
//    3       107,5.0
//    3       105,5.0
//    3       104,5.0
//    3       101,5.0

}