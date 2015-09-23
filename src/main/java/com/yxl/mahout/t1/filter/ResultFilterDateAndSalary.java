package com.yxl.mahout.t1.filter;

import com.yxl.mahout.t1.evaluator.Evaluator;
import com.yxl.mahout.t1.evaluator.RecommendFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 过滤平均工资 小于6000的 且 2013 年前的
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-23 下午4:40
 */
public class ResultFilterDateAndSalary {

    private static final int LIMIT = 6000;

    private static final String PV = System.getProperty("user.dir") + "/src/main/resources/data/pv.csv";
    private static final String JOB = System.getProperty("user.dir") + "/src/main/resources/data/job.csv";

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 2;

    public static void main(String[] args) throws TasteException, IOException {

        DataModel dataModel = RecommendFactory.buildDataModelNoPref(PV);
        RecommenderBuilder rb1 = Evaluator.userCityBlock(dataModel);
        RecommenderBuilder rb2 = Evaluator.itemLoglikelihood(dataModel);

        LongPrimitiveIterator iter = dataModel.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            System.out.println("userCityBlock    =>");
            filter(uid, rb1, dataModel);
            System.out.println("itemLoglikelihood=>");
            filter(uid, rb2, dataModel);
        }
    }

    public static void filter(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel) throws TasteException, IOException {
        Set<Long> outdateJobIDs = getOutdateJobID(JOB);
        Set<Long> salaryJobIDs = getLowSalaryJobID(JOB);

        Set<Long> inter = new HashSet<Long>((ArrayList<Long>)CollectionUtils.intersection(outdateJobIDs,salaryJobIDs));

        IDRescorer rescorer = new JobRescorer(inter);
        List list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM, rescorer);
        RecommendFactory.showItems(uid, list, true);
    }

    public static Set<Long> getOutdateJobID(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        Set<Long> jobids = new HashSet<Long>();
        String s = null;
        while ((s = br.readLine()) != null) {
            String[] cols = s.split(",");
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            Date date = null;
            try {
                date = df.parse(cols[1]);
                if (date.getTime() < df.parse("2013-01-01").getTime()) {
                    jobids.add(Long.parseLong(cols[0]));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
        br.close();
        return jobids;
    }

    public static Set<Long> getLowSalaryJobID(String file) throws IOException {
        File f = new File(file);
        Set<Long> set = new HashSet<Long>();
        if (f.exists() && f.canRead()){
            List<String> rows = FileUtils.readLines(f);
            for(String row : rows){
                String[] cols = StringUtils.split(row,",");
                if (cols == null || cols.length != 3 || Integer.parseInt(cols[2]) >= LIMIT){
                    continue;
                }
                set.add(Long.parseLong(cols[0]));
            }
        }
        return set;

    }

}
