package com.cubead.ncs.matrix.consumer;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.api.DubboResult;
import com.cubead.ncs.matrix.api.PageResult;
import com.cubead.ncs.matrix.api.QuatoSplitCalculationExecutorInf;
import com.cubead.ncs.matrix.api.Quota;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;

public class QuatoSplitCalculationExecutorTest extends BaseTest {

    @Autowired
    private QuatoSplitCalculationExecutorInf quatoSplitCalculationExecutorInf;

    private static QueryUnit roiQueryUnit;
    private static QueryUnit compressedQueryUnit;
    private static QueryUnit pvQueryUnit;

    @Before
    public void initQueryUnit() {

        // roi init
        roiQueryUnit = new QueryUnit();
        roiQueryUnit.setSql(new StringBuilder()
                .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi ")
                .append(" from ca_summary_136191_roi ").append(" where log_day >= 6 AND log_day <= 55 ")
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
    public void calculatAllMergeResultSetAsJsonObjectsTest() {

        roiQueryUnit.setSql(roiQueryUnit.getSql() + " limit 10 ");
        List<JSONObject> josnRows = quatoSplitCalculationExecutorInf.calculatLimitMergeResultSetAsJsonObjects(
                roiQueryUnit, compressedQueryUnit, pvQueryUnit);

        Assert.assertNotNull(josnRows);
        Assert.assertEquals(josnRows.size(), 10);

        logger.info("查询结果合集:{}", josnRows);
        logger.info("数据结果展示:{}", josnRows);

    }

    @Test(expected = IllegalArgumentException.class)
    public void calculatAllMergeResultSetAsJsonObjectsTestInWrongQuato() {

        compressedQueryUnit.setQuotas(Quota.COST);
        quatoSplitCalculationExecutorInf.calculatLimitMergeResultSetAsJsonObjects(roiQueryUnit, compressedQueryUnit,
                pvQueryUnit);

    }

    @Test
    public void calculatLimitMergeResultSetAsJsonObjectsTest() {

        DubboResult<PageResult> josnRows = quatoSplitCalculationExecutorInf.calculatAllMergeResultSetAsJsonObjects(10,
                roiQueryUnit, compressedQueryUnit, pvQueryUnit);

        Assert.assertNotNull(josnRows);
        Assert.assertEquals(josnRows.getBean().getPageResult().size(), 10);

        logger.info("查询结果合集:{}", josnRows.getResultStatus());
        logger.info("数据结果展示:{}", josnRows);
    }
}
