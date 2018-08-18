package com.mobanker.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by liuxinyuan on 2018/2/27.
 */
public class StringToJson extends GenericUDF {

    public Object evaluate(String str) {
        Map obj;
        try {
            obj = JSONObject.parseObject("{'as':10}");

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        obj = new HashMap<String, Object>();
        obj.put("as", "10");

        return  "aaa";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //定义返回类型参数
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
