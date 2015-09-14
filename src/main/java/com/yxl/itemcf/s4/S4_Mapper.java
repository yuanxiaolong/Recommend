package com.yxl.itemcf.s4;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Step4 Mapper 构造以物品为key，分别输出2个矩阵。一个是相似度，另一个是评分。为线性变换做准备
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:17
 */
public class S4_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private String flag;// A同现矩阵 or B评分矩阵

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        flag = split.getPath().getParent().getName();// 判断读的数据集

        System.out.println("FALG is ->>>>>>>>>>>>>>>>>>>" + flag);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = Main.DELIMITER.split(value.toString());

        if (flag.equals("S3_2_output")) {// 同现矩阵
            String[] v1 = tokens[0].split(":");
            String itemID1 = v1[0];
            String itemID2 = v1[1];
            String num = tokens[1];

            Text k = new Text(itemID1);
            Text v = new Text("A:" + itemID2 + "," + num);

            context.write(k, v);

        } else if (flag.equals("S3_1_output")) {// 评分矩阵
            String[] v2 = tokens[1].split(":");
            String itemID = tokens[0];
            String userID = v2[0];
            String pref = v2[1];

            Text k = new Text(itemID);
            Text v = new Text("B:" + userID + "," + pref);

            context.write(k, v);

        }
    }

//    FALG is ->>>>>>>>>>>>>>>>>>>S3_2_output
//    101:
//    A:103,4
//    A:105,2
//    A:101,5
//    A:106,2
//    A:107,1
//    A:104,4
//    A:102,3
//    102:
//    A:101,3
//    A:102,3
//    A:103,3
//    A:104,2
//    A:105,1
//    A:106,1
//    103:
//    A:101,4
//    A:102,3
//    A:103,4
//    A:104,3
//    A:106,2
//    A:105,1
//    104:
//    A:103,3
//    A:107,1
//    A:106,2
//    A:105,2
//    A:104,4
//    A:102,2
//    A:101,4
//    105:
//    A:102,1
//    A:101,2
//    A:103,1
//    A:104,2
//    A:105,2
//    A:106,1
//    A:107,1
//    106:
//    A:102,1
//    A:101,2
//    A:103,2
//    A:104,2
//    A:105,1
//    A:106,2
//    107:
//    A:101,1
//    A:104,1
//    A:105,1
//    A:107,1



//    FALG is ->>>>>>>>>>>>>>>>>>>S3_1_output
//    101:
//    B:1,5.0
//    B:5,4.0
//    B:4,5.0
//    B:3,2.0
//    B:2,2.0
//    102:
//    B:2,2.5
//    B:5,3.0
//    B:1,3.0
//    103:
//    B:5,2.0
//    B:2,5.0
//    B:4,3.0
//    B:1,2.5
//    104:
//    B:4,4.5
//    B:5,4.0
//    B:2,2.0
//    B:3,4.0
//    105:
//    B:5,3.5
//    B:3,4.5
//    106:
//    B:4,4.0
//    B:5,4.0
//    107:
//    B:3,5.0


}
