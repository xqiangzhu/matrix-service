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
     * @param page
     *            当前页，从1开始
     * @param limitInteger
     *            每页数据量大小
     * @param quotaunits
     *            查询单元
     * @return
     */
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Integer limitInteger,
            QueryUnit... quotaunits);

    /**
     * 查询多表结果并合并计算，分页集合返回,按系统默认页码返回
     * 
     * @author kangye
     * @param page
     *            当前页，从1开始
     * @param quotaunits
     *            查询单元
     * @return
     */
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, QueryUnit... quotaunits);

    /**
     * 查询多表结果并合并计算，分页集合返回
     * 
     * @author kangye
     * @param page
     *            当前页，从1开始
     * @param limitInteger
     *            每页数据量大小
     * @param partitionSupport
     *            是否支持分区并行计算
     * @param quotaunits
     *            查询单元
     * @return
     */
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Integer limitInteger,
            Boolean partitionSupport, QueryUnit... quotaunits);

    /**
     * 查询多表结果并合并计算，分页集合返回
     * 
     * @author kangye
     * @param page
     *            当前页，从1开始
     * @param partitionSupport
     *            是否支持分区并行计算
     * @param quotaunits
     *            查询单元
     * @return
     */
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Boolean partitionSupport,
            QueryUnit... quotaunits);

    /**
     * 查询结果合并必要的数据,暂留查询方式
     * 
     * @author kangye
     * @param quotaunits
     * @return
     */
    @Deprecated
    public List<JSONObject> calculatLimitMergeResultSetAsJsonObjects(QueryUnit... quotaunits);

}
