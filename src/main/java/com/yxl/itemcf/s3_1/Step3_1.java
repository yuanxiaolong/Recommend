package com.yxl.itemcf.s3_1;

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
 * 转换用户评分矩阵 把key从用户 转换成 item，以便做itemCF算法矩阵相乘 的 MR操作
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 上午9:11
 */
public class Step3_1 extends Configured implements Tool {

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

        job.setJobName("itemCF-UserScoreVectorChange-Matrix-yxl");
        job.setJarByClass(getClass());

        job.setMapperClass(S3_1_Mapper.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // 数据量小 设置一个 reduce 方便查看


        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true)?0:1;
    }

    // 结果

//    101     5:4.0
//    101     1:5.0
//    101     2:2.0
//    101     3:2.0
//    101     4:5.0
//    102     1:3.0
//    102     5:3.0
//    102     2:2.5
//    103     2:5.0
//    103     5:2.0
//    103     1:2.5
//    103     4:3.0
//    104     2:2.0
//    104     5:4.0
//    104     3:4.0
//    104     4:4.5
//    105     3:4.5
//    105     5:3.5
//    106     5:4.0
//    106     4:4.0
//    107     3:5.0


}
