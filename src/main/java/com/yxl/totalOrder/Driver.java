package com.yxl.totalOrder;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;

import java.io.File;

/**
 * 驱动程序
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-29 下午2:45
 */
public class Driver extends Configured implements Tool {

    public static final String ROOT = "/test/xiaolong.yuanxl/totalOrder";

//    private static final String INPUT = System.getProperty("user.dir") + "/src/main/resources/order/input.txt";

    private static final String INPUT = ROOT + "/input.txt";

    private static final String PARTITION_FILE = ROOT + "/p.lst";

    private static final String STAGING_FILE = ROOT + "/staging";

    private static final String OUTPUT = ROOT + "/out";


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(INPUT);
        Path partitionFile = new Path(PARTITION_FILE);
        Path staging = new Path(STAGING_FILE);
        Path output = new Path(OUTPUT);

        // 删除已有结果集
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(partitionFile)) {
            fs.delete(partitionFile,true);
        }
        if (fs.exists(staging)) {
            fs.delete(staging,true);
        }
        if (fs.exists(output)) {
            fs.delete(output,true);
        }

        Job sampleJob = Job.getInstance();
        sampleJob.setJobName("totalOrder-1");
        sampleJob.setJarByClass(Driver.class);

        sampleJob.setMapperClass(SampleMapper.class);
        sampleJob.setNumReduceTasks(0);

        sampleJob.setOutputKeyClass(Text.class);
        sampleJob.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(sampleJob, input);

        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(sampleJob,staging);

        int code = sampleJob.waitForCompletion(true) ? 0:1;

        if (code == 0){
            Job orderJob = Job.getInstance();
            orderJob.setJarByClass(Driver.class);

            orderJob.setMapperClass(Mapper.class);
            orderJob.setReducerClass(ValueReducer.class);

            orderJob.setNumReduceTasks(1);

            orderJob.setPartitionerClass(TotalOrderPartitioner.class);

            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);

            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, staging);

            TextOutputFormat.setOutputPath(orderJob, output);

            orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");

            InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(0.1, 10));

            code = orderJob.waitForCompletion(true) ? 0 : 2;

        }

        return code;
    }

//    [root@hadoop8 xiaolong.yuanxl]# hadoop fs -ls /test/xiaolong.yuanxl/totalOrder
//    Found 4 items
//    -rw-r--r--   2 root supergroup        401 2015-09-29 16:11 /test/xiaolong.yuanxl/totalOrder/input.txt
//    drwxr-xr-x   - root supergroup          0 2015-09-29 16:13 /test/xiaolong.yuanxl/totalOrder/out
//    -rw-r--r--   2 root supergroup        129 2015-09-29 16:13 /test/xiaolong.yuanxl/totalOrder/p.lst
//    drwxr-xr-x   - root supergroup          0 2015-09-29 16:12 /test/xiaolong.yuanxl/totalOrder/staging

    //可能数据量太小没有采样到
//    [root@hadoop8 xiaolong.yuanxl]# hadoop fs -text /test/xiaolong.yuanxl/totalOrder/p.lst
//    15/09/29 16:14:10 INFO zlib.ZlibFactory: Successfully loaded & initialized native-zlib library
//    15/09/29 16:14:10 INFO compress.CodecPool: Got brand-new decompressor [.deflate]


//    [root@hadoop8 xiaolong.yuanxl]# hadoop fs -ls /test/xiaolong.yuanxl/totalOrder/staging
//    Found 2 items
//    -rw-r--r--   2 root supergroup          0 2015-09-29 15:32 /test/xiaolong.yuanxl/totalOrder/staging/_SUCCESS
//    -rw-r--r--   2 root supergroup       1978 2015-09-29 15:32 /test/xiaolong.yuanxl/totalOrder/staging/part-m-00000


//    [root@hadoop8 xiaolong.yuanxl]# hadoop fs -text /test/xiaolong.yuanxl/totalOrder/staging/part-m-00000
//            073     073
//            053     053
//            045     045
//            015     015
//            017     017
//            026     026
//            095     095
//            085     085
//            076     076
//            016     016
//            014     014
//            062     062
//            019     019
//            084     084
//            020     020
//            024     024
//            065     065
//            035     035
//            001     001
//            052     052
//            027     027
//            064     064
//            066     066
//            025     025
//            032     032
//            056     056
//            023     023
//            028     028
//            069     069
//            093     093
//            003     003
//            090     090
//            010     010
//            039     039
//            050     050
//            042     042
//            098     098
//            097     097
//            013     013
//            087     087
//            054     054
//            059     059
//            004     004
//            033     033
//            046     046
//            086     086
//            057     057
//            060     060
//            038     038
//            047     047
//            018     018
//            072     072
//            083     083
//            094     094
//            049     049
//            051     051
//            055     055
//            048     048
//            041     041
//            089     089
//            080     080
//            037     037
//            030     030
//            071     071
//            077     077
//            079     079
//            040     040
//            008     008
//            005     005
//            091     091
//            096     096
//            075     075
//            034     034
//            011     011
//            099     099
//            022     022
//            063     063
//            012     012
//            068     068
//            067     067
//            058     058
//            029     029
//            002     002
//            070     070
//            007     007
//            081     081
//            021     021
//            061     061
//            043     043
//            006     006
//            044     044
//            100     100
//            092     092
//            088     088
//            078     078
//            036     036
//            031     031
//            009     009
//            082     082
//            074     074


//    [root@hadoop8 xiaolong.yuanxl]# hadoop fs -cat /test/xiaolong.yuanxl/totalOrder/out/part-r-00000
//
//            001
//            002
//            003
//            004
//            005
//            006
//            007
//            008
//            009
//            010
//            011
//            012
//            013
//            014
//            015
//            016
//            017
//            018
//            019
//            020
//            021
//            022
//            023
//            024
//            025
//            026
//            027
//            028
//            029
//            030
//            031
//            032
//            033
//            034
//            035
//            036
//            037
//            038
//            039
//            040
//            041
//            042
//            043
//            044
//            045
//            046
//            047
//            048
//            049
//            050
//            051
//            052
//            053
//            054
//            055
//            056
//            057
//            058
//            059
//            060
//            061
//            062
//            063
//            064
//            065
//            066
//            067
//            068
//            069
//            070
//            071
//            072
//            073
//            074
//            075
//            076
//            077
//            078
//            079
//            080
//            081
//            082
//            083
//            084
//            085
//            086
//            087
//            088
//            089
//            090
//            091
//            092
//            093
//            094
//            095
//            096
//            097
//            098
//            099
//            100

}
