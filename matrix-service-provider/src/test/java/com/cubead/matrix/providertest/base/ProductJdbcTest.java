package com.cubead.matrix.providertest.base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch;
import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch.QuotaField;

/**
 * 水平垂直切分启动类
 * 
 * @author kangye
 */
public class ProductJdbcTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MatrixTableSearch matrixTableSearch;

    // @Test
    public void testMatrixTableSearch() {

        // 维度
        TreeSet<Integer> fieldHashSet = new TreeSet<Integer>();
        Map<String, Double[]> mapValuesMap = new HashMap<>();

        long t1 = System.currentTimeMillis();
        Assert.assertNotNull(matrixTableSearch);
        List<List<QuotaField>> result = matrixTableSearch.getExampleStatistics(fieldHashSet);

        logger.info("查询耗时:{}", (System.currentTimeMillis() - t1) + " ms");
        logger.info("查询结果：{}", result.size());

        for (List<QuotaField> quotaFields : result) {
            logger.info("查询结果：{}", quotaFields.size());
            for (QuotaField quotaField : quotaFields) {
                String key = quotaField.getPaserKeys();
                if (mapValuesMap.containsKey(key)) {
                    Double[] doubles = mapValuesMap.get(key);
                    doubles[quotaField.getQuota().getIndex()] += quotaField.getValue();
                    mapValuesMap.put(key, doubles);
                } else {
                    Double[] doubles = new Double[] { 0.0, 0.0, 0.0, 0.0, 0.0 };
                    doubles[quotaField.getQuota().getIndex()] = quotaField.getValue();
                    mapValuesMap.put(key, doubles);
                }

            }
        }

        logger.info("编码顺序队列:{}", mapValuesMap.size());

        // logger.info(mapValuesMap.toString());
        logger.info("查询和合并耗时:{}", (System.currentTimeMillis() - t1) + " ms");
    }

}
