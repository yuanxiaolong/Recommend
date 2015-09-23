package com.yxl.mahout.t1;

/**
 * 实验 http://blog.fens.me/hadoop-mahout-recommend-job/
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-23 下午2:39
 */
public class Main {

    /**
     * 需求
     * 1. 组合使用推荐算法，选出“评估推荐器”验证得分较高的算法
     * 2. 人工验证推荐结果
     * 3. 职位有时效性，推荐的结果应该是发布半年内的职位
     * 4. 工资的标准，应不低于用户浏览职位工资的平均值的80%
     */

    public static void main(String[] args) {

        /**
         * 组合7种算法
         * userCF1: LogLikelihoodSimilarity + NearestNUserNeighborhood + GenericBooleanPrefUserBasedRecommender
         * userCF2: CityBlockSimilarity+ NearestNUserNeighborhood + GenericBooleanPrefUserBasedRecommender
         * userCF3: UserTanimoto + NearestNUserNeighborhood + GenericBooleanPrefUserBasedRecommender
         * itemCF1: LogLikelihoodSimilarity + GenericBooleanPrefItemBasedRecommender
         * itemCF2: CityBlockSimilarity+ GenericBooleanPrefItemBasedRecommender
         * itemCF3: ItemTanimoto + GenericBooleanPrefItemBasedRecommender
         * slopeOne：SlopeOneRecommender
         */

    }

    // 源数据 data 目录下
    // pv.csv: 职位被浏览的信息,包括用户ID，职位ID
    // job.csv: 职位基本信息,包括职位ID，发布时间，工资标准

}
