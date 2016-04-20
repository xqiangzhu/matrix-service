package com.cubead.ncs.matrix.consumer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.api.DubboResult;
import com.cubead.ncs.matrix.api.DubboResult.ResultStatus;
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
    private static QueryUnit uvQueryUnit;

    @Before
    public void initQueryUnit() {

        // roi init
        roiQueryUnit = new QueryUnit();
        roiQueryUnit.setSql(new StringBuilder()
                .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi ")
                .append(" from ca_summary_136191_roi ").append(SqlRandomGenerator.generteWhereLogDay())
                .append(SqlRandomGenerator.generteGroupSQl()).append(" order by roi ").toString());
        roiQueryUnit.setQuotas(Quota.ROI);

        // compressed
        compressedQueryUnit = new QueryUnit();
        compressedQueryUnit
                .setSql(new StringBuilder()
                        .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(ext_resource_count) ext_resource_count, sum(impressions) impressions ")
                        .append(" from ca_summary_136191_compressed ").append(SqlRandomGenerator.generteWhereLogDay())
                        .append(SqlRandomGenerator.generteGroupSQl()).toString());
        compressedQueryUnit.setQuotas(Quota.IMPRESSIONS, Quota.EXT_RESOURCE_COUNT);

        // pv
        pvQueryUnit = new QueryUnit();
        pvQueryUnit.setSql(new StringBuilder().append("SELECT sub_tenant_id, campaign, adgroup, keyword, count(*) pv ")
                .append(" from ca_summary_136191_compressed ").append(SqlRandomGenerator.generteWhereLogDay())
                .append(SqlRandomGenerator.generteGroupSQl()).toString());
        pvQueryUnit.setQuotas(Quota.PV);

        // uv
        uvQueryUnit = new QueryUnit();
        uvQueryUnit.setSql(new StringBuilder().append("SELECT sub_tenant_id, campaign, adgroup, keyword, count(*) uv ")
                .append(" from ca_summary_136191_uv ").append(SqlRandomGenerator.generteWhereLogDay())
                .append(SqlRandomGenerator.generteGroupSQl()).toString());
        uvQueryUnit.setQuotas(Quota.UV);

    }

    // 容错性测试, 按limit计算
    @Test
    public void calculatAllMergeResultSetAsJsonObjectsTest() {

        roiQueryUnit.setSql(roiQueryUnit.getSql() + " limit 10 ");
        List<JSONObject> josnRows = quatoSplitCalculationExecutorInf.calculatLimitMergeResultSetAsJsonObjects(
                roiQueryUnit, compressedQueryUnit, pvQueryUnit, uvQueryUnit);

        Assert.assertNotNull(josnRows);
        Assert.assertEquals(josnRows.size(), 10);

        logger.info("数据结果展示:{}", josnRows.get(0));

    }

    // 按limit计算,错误的指标
    @Test(expected = IllegalArgumentException.class)
    public void calculatAllMergeResultSetAsJsonObjectsTestInWrongQuato() {

        compressedQueryUnit.setQuotas(Quota.COST);
        quatoSplitCalculationExecutorInf.calculatLimitMergeResultSetAsJsonObjects(roiQueryUnit, compressedQueryUnit,
                pvQueryUnit, uvQueryUnit);

    }

    // 分组计算
    @Test
    public void calculatLimitMergeResultSetAsJsonObjectsTest() {

        DubboResult<PageResult> josnRows = quatoSplitCalculationExecutorInf.calculatAllMergeResultSetAsJsonObjects(1,
                10, roiQueryUnit, compressedQueryUnit, pvQueryUnit, uvQueryUnit);

        Assert.assertNotNull(josnRows);
        Assert.assertEquals(josnRows.getBean().getPageResult().size(), 10);

        logger.info("数据结果展示:{}", josnRows.getBean().getPageResult().get(0));
    }

    // 两次计算同一组查询, 第二次从缓存中取 , 分组计算
    @Test
    public void calculatLimitMergeResultSetAsJsonObjectsInCacheTest() {

        current_time = System.currentTimeMillis();
        DubboResult<PageResult> josnRows = quatoSplitCalculationExecutorInf.calculatAllMergeResultSetAsJsonObjects(1,
                10, roiQueryUnit, compressedQueryUnit, pvQueryUnit, uvQueryUnit);

        Assert.assertNotNull(josnRows);

        logger.info("第一次运算时长:{}ms", System.currentTimeMillis() - current_time);
        logger.info("数据结果展示:{}", josnRows.getBean().getPageResult().get(0));

        logger.info("休眠2秒,确保前一次结果被缓存,开始第二次查询");
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        current_time = System.currentTimeMillis();
        // 第二次查询同样的语句
        josnRows = quatoSplitCalculationExecutorInf.calculatAllMergeResultSetAsJsonObjects(2, 10, roiQueryUnit,
                compressedQueryUnit, pvQueryUnit, uvQueryUnit);

        Assert.assertNotNull(josnRows);
        Assert.assertEquals(josnRows.getBean().getPageResult().size(), 10);

        logger.info("第二次运算时长:{}ms", System.currentTimeMillis() - current_time);
        logger.info("数据结果展示:{}", josnRows.getBean().getPageResult().get(0));

    }

    // 容错性测试 传一个错误的sql 表名错误
    @Test
    public void calculatAllMergeResultSetAsJsonObjectsTestWithWrongSql() {

        String wrongTableName = " from ca_summary_136191_rofgdi ";
        roiQueryUnit.setSql(new StringBuilder()
                .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi ")
                .append(wrongTableName).append(SqlRandomGenerator.generteWhereLogDay())
                .append(SqlRandomGenerator.generteGroupSQl()).append(" order by roi ").toString());

        DubboResult<PageResult> josnRows = quatoSplitCalculationExecutorInf.calculatAllMergeResultSetAsJsonObjects(1,
                10, roiQueryUnit, compressedQueryUnit, pvQueryUnit, uvQueryUnit);

        Assert.assertEquals(josnRows.getResultStatus(), ResultStatus.FAIL);
        logger.info("sql错误提示:{}", josnRows.getMessage());
    }
}
