package com.mobanker.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liuxinyuan on 2018/2/8.
 */
public class StringToJsonArray extends UDF {
    public   JSONArray evaluate(String str) {
        try {
            JSONArray array = JSONArray.parseArray(str);
            return array;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new JSONArray();
    }

    public static void main(String[] args){
    }
}
