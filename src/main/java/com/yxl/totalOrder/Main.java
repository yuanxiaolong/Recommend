package com.yxl.totalOrder;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 全排序
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-29 下午2:37
 */
public class Main {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Driver(),new String[]{""});
    }

    //mock 一个文件 乱序
    private void mock(){
        File file = new File("/Users/xiaolongyuan/Documents/tmp/input.txt");
        try{
            if (!file.exists()){
                file.createNewFile();
            }
            List<Integer> list = new ArrayList<Integer>();
            for (int i = 0; i < 100; i++) {
                list.add(i+1);
            }
            Collections.shuffle(list);
            FileUtils.writeLines(file, list);

            List<String> nonOrderList = FileUtils.readLines(file);
            for (String str : nonOrderList){
                System.out.println(str);
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }

}
