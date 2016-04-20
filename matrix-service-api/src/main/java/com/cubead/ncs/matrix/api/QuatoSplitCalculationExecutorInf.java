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
     * 查询多表结果并合并计算，分页集合返回
     * 
     * @author kangye
     * @param quotaunits
     * @return
     */
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Integer limitInteger,
            QueryUnit... quotaunits);

    /**
     * 查询结果合并必要的数据
     * 
     * @author kangye
     * @param quotaunits
     * @return
     */
    public List<JSONObject> calculatLimitMergeResultSetAsJsonObjects(QueryUnit... quotaunits);

}
