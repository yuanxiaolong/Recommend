package com.yxl.itemcf;

import com.yxl.itemcf.s1.Step1;
import com.yxl.itemcf.s2.Step2;
import com.yxl.itemcf.s3_1.Step3_1;
import com.yxl.itemcf.s3_2.Step3_2;
import com.yxl.itemcf.s4.Step4;

import com.yxl.itemcf.s5.Step5;
import org.apache.hadoop.util.ToolRunner;

import java.util.regex.Pattern;

/**
 * 程序入口点
 *
 * hadoop jar recommend-1.0-SNAPSHOT.jar com.yxl.itemcf.Main
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-09 下午9:29
 */
public class Main {

    public static final String ROOT = "/test/xiaolong.yuanxl/rc";

    public static final Pattern DELIMITER = Pattern.compile("[\t,]"); // 制表符或逗号分隔

    public static void main(String[] args) throws Exception {
        // Step1.java，按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
        // Step2.java，对物品组合列表进行计数，建立物品的同现矩阵
        // Step3_1.java，合并评分矩阵
        // Step3_2.java 合并同现矩阵
        // Step4.java，矩阵相乘
        // Step5.java 做和计算推荐结果列表

        // 代码保证入参 后面不用做数组越界校验
        ToolRunner.run(new Step1(), new String[]{ROOT + "/data/data.csv", ROOT + "/S1_output"});
        ToolRunner.run(new Step2(), new String[]{ROOT + "/S1_output", ROOT + "/S2_output"});
        ToolRunner.run(new Step3_1(), new String[]{ROOT + "/S1_output", ROOT + "/S3_1_output"});
        ToolRunner.run(new Step3_2(), new String[]{ROOT + "/S2_output", ROOT + "/S3_2_output"});
        ToolRunner.run(new Step4(), new String[]{ROOT + "/S3_1_output", ROOT + "/S3_2_output", ROOT + "/S4_output"});
        ToolRunner.run(new Step5(), new String[]{ROOT + "/S4_output", ROOT + "/S5_output"});
        System.exit(0);

    }

    // 原始数据

//    1,101,5.0
//    1,102,3.0
//    1,103,2.5
//    2,101,2.0
//    2,102,2.5
//    2,103,5.0
//    2,104,2.0
//    3,101,2.0
//    3,104,4.0
//    3,105,4.5
//    3,107,5.0
//    4,101,5.0
//    4,103,3.0
//    4,104,4.5
//    4,106,4.0
//    5,101,4.0
//    5,102,3.0
//    5,103,2.0
//    5,104,4.0
//    5,105,3.5
//    5,106,4.0

}
