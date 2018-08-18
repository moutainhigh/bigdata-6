package com.mobanker.udf;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;


/**
 * Created by liuxinyuan on 2018/2/7.
 */
@SuppressWarnings("deprecation")
public class AggreateToArray extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns()) {
            // 函数允许使用“*”查询的时候会返回true。
            throw new SemanticException("不支持使用*查询");
        }
        // 获取函数参数列表
        ObjectInspector[] inspectors = info.getParameterObjectInspectors();
        if (inspectors.length != 1) {
            throw new UDFArgumentException("只支持1个参数进行查询");
        }
        //  AbstractPrimitiveWritableObjectInspector apwoi = (AbstractPrimitiveWritableObjectInspector) inspectors[0];
        return new JsonToArrayEvaluator();
    }

    /**
     * 进行json聚合
     *
     * @author gerry
     */
    static class JsonToArrayEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        Text bigJsonObj = new Text(new JSONObject().toJSONString());

        static class JSONObjAgg implements AggregationBuffer {
            Text jsonObj;
            boolean empty;

            void add(Text text) {
                JSONObject obj = JSONObject.parseObject(text.toString());
                JSONObject bigObj = JSONObject.parseObject(this.jsonObj.toString());
                //   JSONArray array = JSONArray.parseArray(text.toString());
                //     JSONArray bigArray = JSONArray.parseArray(this.jsonArray.toString());
                bigObj.putAll(obj);
                this.jsonObj = new Text(JSONObject.toJSONString(bigObj));
            }
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            System.out.println("开始init");

            super.init(m, parameters);
            System.out.println("parameters's length is :" + parameters.length);
            try {
                this.inputOI = (PrimitiveObjectInspector) parameters[0];
                this.outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Text.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("结束init");

            return this.outputOI;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            System.out.println("开始getNewAggregationBuffer");
            JSONObjAgg sda = new JSONObjAgg();
            this.reset(sda);
            System.out.println("开始getNewAggregationBuffer");
            return sda;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            System.out.println("开始reset");
            JSONObjAgg sda = (JSONObjAgg) agg;
            sda.empty = true;
            sda.jsonObj = new Text(JSONObject.toJSONString(new JSONObject()));
            System.out.println("结束reset");
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            System.out.println("开始iterate");
            if (parameters[0] != null) {
                JSONObjAgg jsonAgg = (JSONObjAgg) agg;
                Object p1 = inputOI.getPrimitiveJavaObject(parameters[0]);
                String[] rs = String.valueOf(p1).split("#");
                JSONObject object = new JSONObject();
                JSONObject bigObject = new JSONObject();
                object.put("engineinputstr", rs[0]);
                object.put("ruleenginename", rs[1]);
                object.put("engineoutputstr", rs[2]);
                bigObject.put(rs[1], object);
                jsonAgg.add(new Text(JSONObject.toJSONString(bigObject)));
            }
            System.out.println("结束iterate");
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            JSONObjAgg objAgg = (JSONObjAgg) agg;
            JSONObject obj = JSONObject.parseObject(objAgg.jsonObj.toString());
            JSONObject bigoBJ = JSONObject.parseObject(this.bigJsonObj.toString());
            bigoBJ.putAll(obj);
            return new Text(bigoBJ.toJSONString());
        }


        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            System.out.println("开始merge");

            if (partial != null) {
                JSONObjAgg sla = (JSONObjAgg) agg;
                Text partialObject = (Text) inputOI.getPrimitiveWritableObject(partial);
                sla.add(partialObject);


            }

        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            JSONObjAgg myagg = (JSONObjAgg) agg;
            bigJsonObj = myagg.jsonObj;
            return myagg.jsonObj;
        }
    }

}
