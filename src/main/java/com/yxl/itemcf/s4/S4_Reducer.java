package com.yxl.itemcf.s4;

import com.yxl.itemcf.Main;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Step4 Reducer 乘法，变换
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 上午12:18
 */
public class S4_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString() + ":");

        Map mapA = new HashMap();
        Map mapB = new HashMap();

        for (Text line : values) {
            String val = line.toString();
            System.out.println(val);

            if (val.startsWith("A:")) {
                String[] kv = Main.DELIMITER.split(val.substring(2));
                mapA.put(kv[0], kv[1]);

            } else if (val.startsWith("B:")) {
                String[] kv = Main.DELIMITER.split(val.substring(2));
                mapB.put(kv[0], kv[1]);

            }
        }

        double result = 0;
        Iterator iter = mapA.keySet().iterator();
        while (iter.hasNext()) {
            String mapk = (String)iter.next();// itemID

            int num = Integer.parseInt((String)mapA.get(mapk));
            Iterator iterb = mapB.keySet().iterator();
            while (iterb.hasNext()) {
                String mapkb = (String)iterb.next();// userID
                double pref = Double.parseDouble((String)mapB.get(mapkb));
                result = num * pref;// 矩阵乘法相乘计算

                Text k = new Text(mapkb);
                Text v = new Text(mapk + "," + result);
                context.write(k, v);

            }
        }
    }

}
