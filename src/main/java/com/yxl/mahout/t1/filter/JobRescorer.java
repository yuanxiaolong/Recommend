package com.yxl.mahout.t1.filter;

import org.apache.mahout.cf.taste.recommender.IDRescorer;

import java.util.Set;

/**
 * author: xiaolong.yuanxl
 * date: 2015-09-23 下午4:40
 */
class JobRescorer implements IDRescorer {
    final private Set jobids;

    public JobRescorer(Set jobs) {
        this.jobids = jobs;
    }

    @Override
    public double rescore(long id, double originalScore) {
        return isFiltered(id) ? Double.NaN : originalScore;
    }

    @Override
    public boolean isFiltered(long id) {
        return jobids.contains(id);
    }
}
