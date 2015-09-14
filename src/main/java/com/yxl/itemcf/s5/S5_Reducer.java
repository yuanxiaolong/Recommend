package com.yxl.itemcf.s5;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Step5 Reducer 同一用户被shuffle一起，进而得到不同物品下的线性变换后的评分，做和
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:20
 */
public class S5_Reducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,Double> map = new HashMap<String,Double>();// 结果

        for (Text line : values) {
            System.out.println(line.toString());
            String[] tokens = Main.DELIMITER.split(line.toString());
            String itemID = tokens[0];
            Double score = Double.parseDouble(tokens[1]);

            if (map.containsKey(itemID)) {
                map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
            } else {
                map.put(itemID, score);
            }
        }

        Iterator iter = map.keySet().iterator();
        while (iter.hasNext()) {
            String itemID = (String)iter.next();
            double score = map.get(itemID);
            Text v = new Text(itemID + "," + score);
            context.write(key, v);
        }

    }

}
