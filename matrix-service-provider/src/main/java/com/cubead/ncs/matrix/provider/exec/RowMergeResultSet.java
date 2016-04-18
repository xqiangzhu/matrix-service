package com.cubead.ncs.matrix.provider.exec;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;

import com.cubead.ncs.matrix.provider.exec.SqlDismantling.QueryUnit;

/**
 * 合并后的结果集
 * 
 * @author kangye
 */
public class RowMergeResultSet {

    private Map<String, Double[]> rowQuotaSetMap = new ConcurrentHashMap<String, Double[]>();
    private TreeSet<String> fieldList = new TreeSet<String>();
    private volatile Boolean limitHasFinished = false;
    private volatile boolean noLimitedUnit = false;

    // 线程是尽量小,减少和DB查询线程抢占CPU
    private static ExecutorService executorService = Executors.newFixedThreadPool(2);

    public RowMergeResultSet() {
        super();
    }

    public Boolean getLimitHasFinished() {
        return limitHasFinished;
    }

    public synchronized void setLimitHasFinished(Boolean limitHasFinished) {
        this.limitHasFinished = limitHasFinished;
    }

    /**
     * 添加结果行,将其合并到, 在limit的情况下不适合这样处理
     * 
     * @author kangye
     * @param sqlRowResultMapping
     * @param IsLimitUnit
     */
    @Deprecated
    public void addRowMergeResult2(final SQLRowResultMapping sqlRowResultMapping) {

        executorService.execute(new Runnable() {
            public void run() {

                Thread.yield();

                final String key = sqlRowResultMapping.getDimension().parseAsKey();
                Double[] values = rowQuotaSetMap.get(key);

                if (null == values) {
                    // 新增
                    values = initZeroFullValues();
                }

                List<QuotaWithValue> quotaWithValues = sqlRowResultMapping.getQuotaWithValues();
                if (CollectionUtils.isEmpty(quotaWithValues))
                    throw new IllegalArgumentException("sqlRowResultMapping不存在指标值");

                for (QuotaWithValue quotaWithValue : quotaWithValues) {
                    Integer seialNumber = quotaWithValue.getQuota().getSeialNumber();
                    values[seialNumber] += quotaWithValue.getValue();
                }

                rowQuotaSetMap.put(key, values);
            }
        });
    }

    public void addRowMergeResult(final SQLRowResultMapping sqlRowResultMapping, final Boolean isLimitUnit) {

        final String key = sqlRowResultMapping.getDimension().parseAsKey();
        Double[] values = rowQuotaSetMap.get(key);

        if (null == values) {
            // 在有limit情况下，如果在原集合中未找到将不做任何处理
            if (isLimitUnit == false && limitHasFinished == true)
                return;

            // 新增
            values = initZeroFullValues();
        }

        List<QuotaWithValue> quotaWithValues = sqlRowResultMapping.getQuotaWithValues();
        if (CollectionUtils.isEmpty(quotaWithValues))
            throw new IllegalArgumentException("sqlRowResultMapping不存在指标值");

        for (QuotaWithValue quotaWithValue : quotaWithValues) {
            Integer seialNumber = quotaWithValue.getQuota().getSeialNumber();
            values[seialNumber] += quotaWithValue.getValue();
        }

        rowQuotaSetMap.put(key, values);
    }

    public static Double[] initZeroFullValues() {

        Quota[] quotas = Quota.values();
        Double[] doubles = new Double[quotas.length];

        for (int i = 0; i < quotas.length; i++) {
            doubles[i] = 0.0;
        }

        return doubles;
    }

    public Map<String, Double[]> getRowQuotaSetMap() {
        return rowQuotaSetMap;
    }

    public TreeSet<String> getFieldList() {
        return fieldList;
    }

    public SqlDismantling[] validateQueryUnitGroup(QueryUnit... quotaunits) {

        if (null == quotaunits)
            throw new IllegalArgumentException("未找到queryUnit,请检查sql");

        SqlDismantling[] sqlDismantlings = new SqlDismantling[quotaunits.length];

        int limitUnitCount = 0;
        for (int i = 0; i < quotaunits.length; i++) {

            SqlDismantling sqlDismantling = new SqlDismantling(quotaunits[i]);
            sqlDismantlings[i] = sqlDismantling;

            if (i == 0) {
                fieldList = sqlDismantling.getFields();
            } else {

                if (!CollectionUtils.isEqualCollection(fieldList, sqlDismantling.getFields())) {
                    throw new IllegalArgumentException("集合间维度存在不一致,请检查sql");
                }
            }

            if (sqlDismantling.isLimitUnit())
                limitUnitCount++;
        }

        if (limitUnitCount > 1) {
            throw new IllegalArgumentException("queryUnit 中在多个limit");
        }

        if (limitUnitCount == 0) {
            limitHasFinished = true;
        }

        return sqlDismantlings;
    }

}
