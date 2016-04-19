package com.cubead.ncs.matrix.provider.out;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.api.DubboResult;
import com.cubead.ncs.matrix.api.DubboResult.ResultStatus;
import com.cubead.ncs.matrix.api.PageResult;
import com.cubead.ncs.matrix.api.QuatoSplitCalculationExecutorInf;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;
import com.cubead.ncs.matrix.provider.compress.RowMergeResultTransform;
import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch;
import com.cubead.ncs.matrix.provider.exec.QuatoSplitCalculationExecutor;
import com.cubead.ncs.matrix.provider.exec.QuatoSplitCalculationWithCountExecutor;
import com.cubead.ncs.matrix.provider.exec.RowMergeResultSet;

@Component
public class QuatoSplitCalculationExecutorImpl implements QuatoSplitCalculationExecutorInf {

    private static final Logger logger = LoggerFactory.getLogger(MatrixTableSearch.class);

    @Autowired
    private QuatoSplitCalculationExecutor quatoSplitCalculationExecutor;

    @Autowired
    private QuatoSplitCalculationWithCountExecutor quatoSplitCalculationWithCountExecutor;

    @Autowired
    private RowMergeResultTransform resultTransform;

    @Override
    public List<JSONObject> calculatLimitMergeResultSetAsJsonObjects(QueryUnit... quotaunits) {

        if (null == quotaunits)
            throw new IllegalArgumentException("未参入任何参数");

        // 获取结果集
        RowMergeResultSet rowMergeResultSet = quatoSplitCalculationExecutor.calculatAllMergeResultSet(quotaunits);
        logger.debug("获取结果集:{}", rowMergeResultSet.getRowQuotaSetMap().size());

        // 将结果转化为JSON串数组
        List<JSONObject> josnRows = resultTransform.transFormRowResultSetAsAJsonObjects(rowMergeResultSet);
        logger.debug("将结果转化为JSON串数组:{}", josnRows.size());

        return josnRows;
    }

    @Override
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer limit, QueryUnit... quotaunits) {

        DubboResult<PageResult> dubboResult = new DubboResult<>();
        PageResult pageResult = new PageResult();
        dubboResult.setMessageAndStatus("查询成功", ResultStatus.SUCCESS);

        if (null == quotaunits) {
            dubboResult.setMessageAndStatus("未参入任何参数", ResultStatus.FAIL);
        }

        try {

            // 获取结果集
            RowMergeResultSet rowMergeResultSet = quatoSplitCalculationWithCountExecutor
                    .calculatAllMergeResultSet(quotaunits);
            int size = rowMergeResultSet.getRowQuotaSetMap().size();
            pageResult.setCount(size);

            // 将结果转化为JSON串数组
            List<JSONObject> josnRows = resultTransform.transFormRowResultSetAsAJsonObjects(limit, rowMergeResultSet);
            pageResult.setPageResult(josnRows.subList(0, limit));
            dubboResult.setBean(pageResult);

        } catch (Exception e) {
            dubboResult.setMessageAndStatus(e.getMessage(), ResultStatus.FAIL);
        }

        return dubboResult;
    }
}
