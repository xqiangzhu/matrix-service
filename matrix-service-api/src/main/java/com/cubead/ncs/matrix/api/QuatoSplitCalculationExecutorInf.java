package com.cubead.ncs.matrix.api;

import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;

/**
 * 对外服务：合并查询和计算
 * 
 * @author kangye
 */
public interface QuatoSplitCalculationExecutorInf {

    /**
     * 查询多表结果并合并计算，以jsonobject集合返回
     * 
     * @author kangye
     * @param quotaunits
     * @return
     */
    List<JSONObject> calculatAllMergeResultSetAsJsonObjects(QueryUnit... quotaunits);
}
