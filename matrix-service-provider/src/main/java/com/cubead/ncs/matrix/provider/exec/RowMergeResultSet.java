package com.cubead.ncs.matrix.provider.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;

import com.cubead.ncs.matrix.api.DubboResult;
import com.cubead.ncs.matrix.api.PageResult;
import com.cubead.ncs.matrix.api.Quota;
import com.cubead.ncs.matrix.api.QuotaWithValue;
import com.cubead.ncs.matrix.api.SqlDismantling;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;

/**
 * 合并后的结果集
 * 
 * @author kangye
 */
public class RowMergeResultSet implements Serializable {

    private static final long serialVersionUID = 1163471114733383569L;
    private Map<String, Double[]> rowQuotaSetMap = new ConcurrentHashMap<String, Double[]>();
    private TreeSet<String> fieldList = new TreeSet<String>();
    private volatile Boolean limitHasFinished = false;
    private volatile boolean exitLimitedUnit = false;
    private volatile List<String> orderKeys = new ArrayList<>(20000);
    private volatile DubboResult<PageResult> dubboResult = new DubboResult<PageResult>();

    public DubboResult<PageResult> getDubboResult() {
        return dubboResult;
    }

    public void setDubboResult(DubboResult<PageResult> dubboResult) {
        this.dubboResult = dubboResult;
    }

    public RowMergeResultSet() {
        super();
    }

    public Boolean getLimitHasFinished() {
        return limitHasFinished;
    }

    public synchronized void setLimitHasFinished(Boolean limitHasFinished) {
        this.limitHasFinished = limitHasFinished;
    }

    public boolean exitLimitedUnit() {
        return exitLimitedUnit;
    }

    public List<String> getOrderKeys() {
        return orderKeys;
    }

    /**
     * 添加结果行,将其合并到, 在limit的情况下不适合这样处理
     * 
     * @author kangye
     * @param sqlRowResultMapping
     * @param IsLimitUnit
     */
    public void addRowMergeResultWithAllCount(final SQLRowResultMapping sqlRowResultMapping, final boolean isOrderUnit) {

        final String key = sqlRowResultMapping.getDimension().parseAsKey();

        // 这里不需要考虑同步问题，业务限制只有一个order任务，会被顺序插入
        if (isOrderUnit) {
            orderKeys.add(key);
        }

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

        // executorService.execute(new Runnable() {
        // public void run() {
        //
        // Thread.yield();
        //
        // }
        // });
    }

    public void addRowMergeResult(final SQLRowResultMapping sqlRowResultMapping, final Boolean isLimitUnit) {

        final String key = sqlRowResultMapping.getDimension().parseAsKey();
        Double[] values = rowQuotaSetMap.get(key);

        if (null == values) {

            // 当存在limit,limit已经计算完,对非limit结果如果新数据在原set中不存在,直接返回,不会合并
            if (exitLimitedUnit && limitHasFinished && isLimitUnit == false)
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

        if (limitUnitCount == 1) {
            exitLimitedUnit = true;
        }

        return sqlDismantlings;
    }

}
