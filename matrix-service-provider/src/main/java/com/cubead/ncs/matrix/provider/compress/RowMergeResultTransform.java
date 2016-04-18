package com.cubead.ncs.matrix.provider.compress;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.provider.exec.Dimension;
import com.cubead.ncs.matrix.provider.exec.Quota;
import com.cubead.ncs.matrix.provider.exec.RowMergeResultSet;

/**
 * 结果转化工具类
 * 
 * @author kangye
 */
@Component
public class RowMergeResultTransform {

    // TODO 注入redis缓存
    //

    /**
     * 将结果转化为JSON串数组
     * 
     * @author kangye
     * @param rowMergeResultSet
     * @return
     */
    public List<JSONObject> transFormRowResultSetAsAJsonObjects(RowMergeResultSet rowMergeResultSet) {

        if (rowMergeResultSet == null)
            return null;

        List<JSONObject> rows = new ArrayList<>();
        Map<String, Double[]> rowMapSetMap = rowMergeResultSet.getRowQuotaSetMap();

        for (String key : rowMapSetMap.keySet()) {
            RowMergeResult rowMergeResult = new RowMergeResult(key, rowMapSetMap.get(key));
            rows.add(transFormRowResultAsAJsonObject(rowMergeResult, rowMergeResultSet.getFieldList()));
        }

        return rows;
    }

    public JSONObject transFormRowResultAsAJsonObject(RowMergeResult row, TreeSet<String> fields) {

        JSONObject jsonObject = new JSONObject();

        // 根据映射ID, 将数据从设置维度值 如 { provice : 北京, ... }
        String[] fieldValues = row.getRowKey().split(Dimension.SPLIT_SIGN);
        int i = 0;
        for (String field : fields) {
            jsonObject.put(field, getMappingValueFromCache(field, fieldValues[i++]));
        }

        // 设置指标的值 如 {roi : 56, pv : 25}
        for (Quota quota : Quota.values()) {
            jsonObject.put(quota.getQuota(), row.getQuatoValues()[quota.getSeialNumber()]);
        }

        return jsonObject;
    }

    /**
     * 从缓存中读取真是的映射数据
     */
    public String getMappingValueFromCache(String field, String value) {

        // TODO 请在这里完成映射加载逻辑

        if ("adgroup".equals(field)) {
            return "北京";
        } else if ("campaign".equals(field)) {
            return "英语";
        } else if ("keyword".equals(field)) {
            return "出国留学";
        }

        return "field";
    }

    public static class RowMergeResult {

        // 压缩的行串如 15-4545-2121-2121
        private String rowKey;
        private Double[] quatoValues;

        public String getRowKey() {
            return rowKey;
        }

        public void setRowKey(String rowKey) {
            this.rowKey = rowKey;
        }

        public Double[] getQuatoValues() {
            return quatoValues;
        }

        public void setQuatoValues(Double[] quatoValues) {
            this.quatoValues = quatoValues;
        }

        public RowMergeResult(String rowKey, Double[] quatoValues) {
            super();
            this.rowKey = rowKey;
            this.quatoValues = quatoValues;
        }

    }
}
