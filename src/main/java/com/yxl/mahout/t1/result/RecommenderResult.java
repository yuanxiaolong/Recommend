package com.yxl.mahout.t1.result;

import com.yxl.mahout.t1.evaluator.Evaluator;
import com.yxl.mahout.t1.evaluator.RecommendFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;

import java.io.IOException;
import java.util.List;

/**
 * 为得到差异化结果，我们分别取UserCityBlock,itemLoglikelihood，对推荐结果人工比较
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-23 下午4:26
 */
public class RecommenderResult {

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws TasteException, IOException {
        String file = System.getProperty("user.dir") + "/src/main/resources/data/pv.csv";
        DataModel dataModel = RecommendFactory.buildDataModelNoPref(file);
        RecommenderBuilder rb1 = Evaluator.userCityBlock(dataModel);
        RecommenderBuilder rb2 = Evaluator.itemLoglikelihood(dataModel);

        LongPrimitiveIterator iter = dataModel.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            System.out.print("userCityBlock    =>");
            result(uid, rb1, dataModel);
            System.out.print("itemLoglikelihood=>");
            result(uid, rb2, dataModel);
        }
    }

    public static void result(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel) throws TasteException {
        List list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM);
        RecommendFactory.showItems(uid, list, false);
    }

}
