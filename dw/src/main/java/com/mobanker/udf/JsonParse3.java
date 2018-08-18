package com.mobanker.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liuxinyuan on 2017/12/21.
 */
public class JsonParse3 extends UDF {
    /**
     * 获取json数数组第n个元素的prop属性
     *
     * @param str
     * @param
     * @return
     */

    public String evaluate(String str,int index,String key) {
        try {
            JSONArray array = JSONArray.parseArray(str);
            JSONObject obj = array.getJSONObject(index);
            return obj.getString(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
