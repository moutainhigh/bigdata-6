package com.mobanker.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liuxinyuan on 2018/3/1.
 */
public class CleanJson extends UDF {

    public String evaluate(String str) {
        if (str != null) {
            return str.replace("\"{", "{").replace("}\"", "}").replace("\\", "");
        }
        return null;
    }
}
