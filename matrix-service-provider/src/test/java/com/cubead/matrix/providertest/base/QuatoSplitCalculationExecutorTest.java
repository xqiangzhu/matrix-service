package com.cubead.matrix.providertest.base;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSONObject;
import com.cubead.performance.compress.RowMergeResultTransform;
import com.cubead.performance.martix.QuatoSplitCalculationExecutor;
import com.cubead.performance.martix.Quota;
import com.cubead.performance.martix.RowMergeResultSet;
import com.cubead.performance.martix.SqlDismantling.QueryUnit;

/**
 * 垂直压缩方案启动类
 * 
 * @author kangye
 */
public class QuatoSplitCalculationExecutorTest extends BaseTest {

    @Autowired
    private QuatoSplitCalculationExecutor quatoSplitCalculationExecutor;

    @Autowired
    private RowMergeResultTransform resultTransform;

    private static QueryUnit roiQueryUnit;
    private static QueryUnit compressedQueryUnit;
    private static QueryUnit pvQueryUnit;

    @BeforeClass
    public static void initQueryUnit() {

        // roi init
        roiQueryUnit = new QueryUnit();
        roiQueryUnit.setSql(new StringBuilder()
                .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi ")
                .append(" from ca_summary_136191_roi ").append(" where log_day >= 10000 AND log_day <= 100005 ")
                .append(" GROUP BY sub_tenant_id, campaign, adgroup, keyword  order by roi").toString());
        roiQueryUnit.setQuotas(Quota.ROI);

        // compressed
        compressedQueryUnit = new QueryUnit();
        compressedQueryUnit
                .setSql(new StringBuilder()
                        .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(ext_resource_count) ext_resource_count, sum(impressions) impressions ")
                        .append(" from ca_summary_136191_compressed ").append(" where log_day >= 6 AND log_day <= 55 ")
                        .append(" GROUP BY sub_tenant_id, campaign, adgroup, keyword").toString());
        compressedQueryUnit.setQuotas(Quota.IMPRESSIONS, Quota.EXT_RESOURCE_COUNT);

        // pv
        pvQueryUnit = new QueryUnit();
        pvQueryUnit.setSql(new StringBuilder().append("SELECT sub_tenant_id, campaign, adgroup, keyword, count(*) pv ")
                .append(" from ca_summary_136191_compressed ").append(" where log_day >= 6 AND log_day <= 55 ")
                .append(" GROUP BY sub_tenant_id, campaign, adgroup, keyword ").toString());
        pvQueryUnit.setQuotas(Quota.PV);

    }

    @Test
    public void calculatAllMergeResultSetTest() {

        Assert.assertNotNull(quatoSplitCalculationExecutor);

        // 获取结果集
        RowMergeResultSet rowMergeResultSet = quatoSplitCalculationExecutor.calculatAllMergeResultSet(roiQueryUnit,
                compressedQueryUnit, pvQueryUnit);

        // 将结果转化为JSON串数组
        List<JSONObject> josnRows = resultTransform.transFormRowResultSetAsAJsonObjects(rowMergeResultSet);

        logger.info("查询结果合集:{}", josnRows.size());
        logger.info("数据结果展示:{}", CollectionUtils.isEmpty(josnRows) ? null : josnRows.get(0));
    }
}
