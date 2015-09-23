package com.yxl.mahout.t1.filter;

import com.yxl.mahout.t1.evaluator.Evaluator;
import com.yxl.mahout.t1.evaluator.RecommendFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 过滤 2013 年以前的
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-23 下午4:32
 */
public class ResultFilterOutdate {

    private static final String PV = System.getProperty("user.dir") + "/src/main/resources/data/pv.csv";
    private static final String JOB = System.getProperty("user.dir") + "/src/main/resources/data/job.csv";

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws TasteException, IOException {

        DataModel dataModel = RecommendFactory.buildDataModelNoPref(PV);
        RecommenderBuilder rb1 = Evaluator.userCityBlock(dataModel);
        RecommenderBuilder rb2 = Evaluator.itemLoglikelihood(dataModel);

        LongPrimitiveIterator iter = dataModel.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            System.out.println("userCityBlock    =>");
            filterOutdate(uid, rb1, dataModel);
            System.out.println("itemLoglikelihood=>");
            filterOutdate(uid, rb2, dataModel);
        }
    }

    public static void filterOutdate(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel) throws TasteException, IOException {
        Set jobids = getOutdateJobID(JOB);
        IDRescorer rescorer = new JobRescorer(jobids);
        List list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM, rescorer);
        RecommendFactory.showItems(uid, list, true);
    }

    public static Set getOutdateJobID(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        Set jobids = new HashSet();
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


}
