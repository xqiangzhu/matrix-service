package com.cubead.ncs.matrix.provider.exec;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据库取数后的映射结果
 * 
 * @author kangye
 */
public class SQLRowResultMapping {

    // 维度
    private Dimension dimension;

    // 指标值
    private List<QuotaWithValue> quotaWithValues;

    public Dimension getDimension() {
        return dimension;
    }

    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    public List<QuotaWithValue> getQuotaWithValues() {
        return quotaWithValues;
    }

    public void setQuotaWithValues(List<QuotaWithValue> quotaWithValues) {
        this.quotaWithValues = quotaWithValues;
    }

    public SQLRowResultMapping(Dimension dimension) {
        super();
        this.dimension = dimension;
        this.quotaWithValues = new ArrayList<>();
    }

    public boolean isSameRow(SQLRowResultMapping sqlRowResultMapping) {

        String key = this.dimension.parseAsKey();
        String otherKey = sqlRowResultMapping.getDimension().parseAsKey();

        if (null == key || null == otherKey)
            return false;
        if (key.equals(otherKey))
            return true;

        return false;
    }

}
