package com.cubead.ncs.matrix.provider.compress;

import java.util.List;

import org.springframework.stereotype.Component;

import com.cubead.ncs.matrix.compress.api.CompresServiceInft;

@Component
public class CompresService implements CompresServiceInft {

    public List<MapSt> getFeildCompressValue(List<MapStyle> fields) {

        // TODO 对外暴露一个根据字段和原值取它的映射值（压缩后的值）

        // 1. 数据校验 比如字段不存在
        // 2. 查询缓存存在就直接返回
        // 3. 不存在就插入数据，返回插入的ID
        // 4. 将这个新增加的数据存入缓存

        return null;
    }

    public String getOridenByCompressValue(String field, Object value) {

        // TODO 对外暴露一个根据字段和原值取它的映射值（压缩后的值）

        return "";
    }

    // 提升性能，一次能取到同类型的映射
    class MapStyle {
        String field;
        Object value;
    }

    //
    class MapSt {
        MapStyle mapStyle;
        Integer id;
    }

}
