package com.cubead.ncs.matrix.provider.out;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.api.QuatoSplitCalculationExecutorInf;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;
import com.cubead.ncs.matrix.provider.compress.RowMergeResultTransform;
import com.cubead.ncs.matrix.provider.exec.QuatoSplitCalculationExecutor;
import com.cubead.ncs.matrix.provider.exec.RowMergeResultSet;

public class QuatoSplitCalculationExecutorImpl implements QuatoSplitCalculationExecutorInf {

    @Autowired
    private QuatoSplitCalculationExecutor quatoSplitCalculationExecutor;

    @Autowired
    private RowMergeResultTransform resultTransform;

    @Override
    public List<JSONObject> calculatAllMergeResultSetAsJsonObjects(QueryUnit... quotaunits) {

        if (null == quotaunits)
            throw new IllegalArgumentException("未参入任何参数");

        // 获取结果集
        RowMergeResultSet rowMergeResultSet = quatoSplitCalculationExecutor.calculatAllMergeResultSet(quotaunits);

        // 将结果转化为JSON串数组
        List<JSONObject> josnRows = resultTransform.transFormRowResultSetAsAJsonObjects(rowMergeResultSet);

        return josnRows;
    }

}
