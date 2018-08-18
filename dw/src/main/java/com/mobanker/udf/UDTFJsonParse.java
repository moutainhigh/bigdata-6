package com.mobanker.udf;

/**
 * Created by liuxinyuan on 2017/12/28.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class UDTFJsonParse extends GenericUDTF {
    private static final int field_num = 104;
    private static final String[] types = new String[]{
            "P2P网贷",
            "一般消费分期平台",
            "互联网金融门户",
            "信用卡中心",
            "大型消费金融公司",
            "小额贷款公司",
            "理财机构",
            "第三方服务商",
            "财产保险",
            "银行消费金融公司"
    };
    private static final String[] rules = new String[]{
            "7天内申请人在多个平台申请借款",
            "1个月内申请人在多个平台申请借款",
            "3个月内申请人在多个平台申请借款",
            "6个月内申请人在多个平台申请借款",
            "12个月内申请人在多个平台申请借款",
            "18个月内申请人在多个平台申请借款",
            "24个月内申请人在多个平台申请借款",
            "60个月以上申请人在多个平台申请借款"
    };
    private static final String[][] colums = new String[][]{
            {"借款人手机个数", "借款人手机匹配借款事件的借款人手机关联的合作方个数"},
            {"借款人身份证个数", "借款人身份证匹配借款事件的借款人身份证关联的合作方个数"},
            {"个数", "个数"}
    };
    private static final String[] fields = new String[]{"td_7days_ph_cnt",
            "td_7days_id_cnt",
            "td_7days_tot_cnt",
            "td_7days_tot_cnt_p2p",
            "td_7days_tot_cnt_cons",
            "td_7days_tot_cnt_interloan",
            "td_7days_tot_cnt_card",
            "td_7days_tot_cnt_bigcons",
            "td_7days_tot_cnt_minloan",
            "td_7days_tot_cnt_fin",
            "td_7days_tot_cnt_third",
            "td_7days_tot_cnt_insure",
            "td_7days_tot_cnt_bank_cons",
            "td_1m_ph_cnt",
            "td_1m_id_cnt",
            "td_1m_tot_cnt",
            "td_1m_tot_cnt_p2p",
            "td_1m_tot_cnt_cons",
            "td_1m_tot_cnt_interloan",
            "td_1m_tot_cnt_card",
            "td_1m_tot_cnt_bigcons",
            "td_1m_tot_cnt_minloan",
            "td_1m_tot_cnt_fin",
            "td_1m_tot_cnt_third",
            "td_1m_tot_cnt_insure",
            "td_1m_tot_cnt_bank_cons",
            "td_3m_ph_cnt",
            "td_3m_id_cnt",
            "td_3m_tot_cnt",
            "td_3m_tot_cnt_p2p",
            "td_3m_tot_cnt_cons",
            "td_3m_tot_cnt_interloan",
            "td_3m_tot_cnt_card",
            "td_3m_tot_cnt_bigcons",
            "td_3m_tot_cnt_minloan",
            "td_3m_tot_cnt_fin",
            "td_3m_tot_cnt_third",
            "td_3m_tot_cnt_insure",
            "td_3m_tot_cnt_bank_cons",
            "td_6m_ph_cnt",
            "td_6m_id_cnt",
            "td_6m_tot_cnt",
            "td_6m_tot_cnt_p2p",
            "td_6m_tot_cnt_cons",
            "td_6m_tot_cnt_interloan",
            "td_6m_tot_cnt_card",
            "td_6m_tot_cnt_bigcons",
            "td_6m_tot_cnt_minloan",
            "td_6m_tot_cnt_fin",
            "td_6m_tot_cnt_third",
            "td_6m_tot_cnt_insure",
            "td_6m_tot_cnt_bank_cons",
            "td_12m_ph_cnt",
            "td_12m_id_cnt",
            "td_12m_tot_cnt",
            "td_12m_tot_cnt_p2p",
            "td_12m_tot_cnt_cons",
            "td_12m_tot_cnt_interloan",
            "td_12m_tot_cnt_card",
            "td_12m_tot_cnt_bigcons",
            "td_12m_tot_cnt_minloan",
            "td_12m_tot_cnt_fin",
            "td_12m_tot_cnt_third",
            "td_12m_tot_cnt_insure",
            "td_12m_tot_cnt_bank_cons",
            "td_18m_ph_cnt",
            "td_18m_id_cnt",
            "td_18m_tot_cnt",
            "td_18m_tot_cnt_p2p",
            "td_18m_tot_cnt_cons",
            "td_18m_tot_cnt_interloan",
            "td_18m_tot_cnt_card",
            "td_18m_tot_cnt_bigcons",
            "td_18m_tot_cnt_minloan",
            "td_18m_tot_cnt_fin",
            "td_18m_tot_cnt_third",
            "td_18m_tot_cnt_insure",
            "td_18m_tot_cnt_bank_cons",
            "td_24m_ph_cnt",
            "td_24m_id_cnt",
            "td_24m_tot_cnt",
            "td_24m_tot_cnt_p2p",
            "td_24m_tot_cnt_cons",
            "td_24m_tot_cnt_interloan",
            "td_24m_tot_cnt_card",
            "td_24m_tot_cnt_bigcons",
            "td_24m_tot_cnt_minloan",
            "td_24m_tot_cnt_fin",
            "td_24m_tot_cnt_third",
            "td_24m_tot_cnt_insure",
            "td_24m_tot_cnt_bank_cons",
            "td_60m_ph_cnt",
            "td_60m_id_cnt",
            "td_60m_tot_cnt",
            "td_60m_tot_cnt_p2p",
            "td_60m_tot_cnt_cons",
            "td_60m_tot_cnt_interloan",
            "td_60m_tot_cnt_card",
            "td_60m_tot_cnt_bigcons",
            "td_60m_tot_cnt_minloan",
            "td_60m_tot_cnt_fin",
            "td_60m_tot_cnt_third",
            "td_60m_tot_cnt_insure",
            "td_60m_tot_cnt_bank_cons"};
    public static Map<String, String> fieldsMap = new HashMap<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("参数异常");
        }
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        for (int i = 0; i < field_num; i++) {
            fieldNames.add(fields[i]);
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        this.buildMap(fieldsMap);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args == null || args.length != 1) {
            return;
        }
        // 只有一个参数的情况
        String line = args[0].toString();
        Map<String, String> map = transfoerContent2Map(line);
        List<String> result = new ArrayList<>();
        if (map != null) {
            for (String value : fieldsMap.values()) {
                result.add(map.get(value));
            }
            super.forward(result.toArray(new String[0]));
        } else {
            super.forward(result.toArray(new String[0]));
        }
    }

    @Override
    public void close() throws HiveException {
        // nothing

    }

    /**
     * 转换字符串为map对象
     *
     * @param content
     * @return
     */
    public Map<String, String> transfoerContent2Map(String content) {
        Map<String, String> map = new HashMap<>();
        JSONArray jsonArray;
        JSONArray array = null;
        try {
            jsonArray = JSONArray.parseArray(content);
            array = jsonArray.getJSONObject(0).getJSONArray("规则列表");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        for (int i = 0; i < array.size(); i++) {
            JSONObject jsonObject = null;
            try {
                jsonObject = array.getJSONObject(i);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
            for (int j = 0; j < colums.length; j++) {
                try {
                    String key = jsonObject.getString("规则名称") + "_" + colums[j][0];
                    JSONObject cache = jsonObject.getJSONArray("规则详情").getJSONObject(0);
                    String value = cache.getString(colums[j][0]);
                    value = value != null ? value : cache.getString(colums[j][1]);
                    map.put(fieldsMap.get(key), value);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }

            for (int k = 0; k < types.length; k++) {
                try {
                    String key = jsonObject.getString("规则名称") + "_" + types[k];
                    String value = jsonObject.getJSONArray("规则详情").getJSONObject(0).getJSONArray("详情").getJSONObject(0).getString(types[k]);
                    map.put(fieldsMap.get(key), value);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }
        return map;
    }

    public void buildMap(Map<String, String> map) {
        map.put("7天内申请人在多个平台申请借款_借款人手机个数", "td_7days_ph_cnt");
        map.put("7天内申请人在多个平台申请借款_借款人身份证个数", "td_7days_id_cnt");
        map.put("7天内申请人在多个平台申请借款_个数", "td_7days_tot_cnt");
        map.put("7天内申请人在多个平台申请借款_P2P网贷", "td_7days_tot_cnt_p2p");
        map.put("7天内申请人在多个平台申请借款_一般消费分期平台", "td_7days_tot_cnt_cons");
        map.put("7天内申请人在多个平台申请借款_互联网金融门户", "td_7days_tot_cnt_interloan");
        map.put("7天内申请人在多个平台申请借款_信用卡中心", "td_7days_tot_cnt_card");
        map.put("7天内申请人在多个平台申请借款_大型消费金融公司", "td_7days_tot_cnt_bigcons");
        map.put("7天内申请人在多个平台申请借款_小额贷款公司", "td_7days_tot_cnt_minloan");
        map.put("7天内申请人在多个平台申请借款_理财机构", "td_7days_tot_cnt_fin");
        map.put("7天内申请人在多个平台申请借款_第三方服务商", "td_7days_tot_cnt_third");
        map.put("7天内申请人在多个平台申请借款_财产保险", "td_7days_tot_cnt_insure");
        map.put("7天内申请人在多个平台申请借款_银行消费金融公司", "td_7days_tot_cnt_bank_cons");
        map.put("1个月内申请人在多个平台申请借款_借款人手机个数", "td_1m_ph_cnt");
        map.put("1个月内申请人在多个平台申请借款_借款人身份证个数", "td_1m_id_cnt");
        map.put("1个月内申请人在多个平台申请借款_个数", "td_1m_tot_cnt");
        map.put("1个月内申请人在多个平台申请借款_P2P网贷", "td_1m_tot_cnt_p2p");
        map.put("1个月内申请人在多个平台申请借款_一般消费分期平台", "td_1m_tot_cnt_cons");
        map.put("1个月内申请人在多个平台申请借款_互联网金融门户", "td_1m_tot_cnt_interloan");
        map.put("1个月内申请人在多个平台申请借款_信用卡中心", "td_1m_tot_cnt_card");
        map.put("1个月内申请人在多个平台申请借款_大型消费金融公司", "td_1m_tot_cnt_bigcons");
        map.put("1个月内申请人在多个平台申请借款_小额贷款公司", "td_1m_tot_cnt_minloan");
        map.put("1个月内申请人在多个平台申请借款_理财机构", "td_1m_tot_cnt_fin");
        map.put("1个月内申请人在多个平台申请借款_第三方服务商", "td_1m_tot_cnt_third");
        map.put("1个月内申请人在多个平台申请借款_财产保险", "td_1m_tot_cnt_insure");
        map.put("1个月内申请人在多个平台申请借款_银行消费金融公司", "td_1m_tot_cnt_bank_cons");
        map.put("3个月内申请人在多个平台申请借款_借款人手机个数", "td_3m_ph_cnt");
        map.put("3个月内申请人在多个平台申请借款_借款人身份证个数", "td_3m_id_cnt");
        map.put("3个月内申请人在多个平台申请借款_个数", "td_3m_tot_cnt");
        map.put("3个月内申请人在多个平台申请借款_P2P网贷", "td_3m_tot_cnt_p2p");
        map.put("3个月内申请人在多个平台申请借款_一般消费分期平台", "td_3m_tot_cnt_cons");
        map.put("3个月内申请人在多个平台申请借款_互联网金融门户", "td_3m_tot_cnt_interloan");
        map.put("3个月内申请人在多个平台申请借款_信用卡中心", "td_3m_tot_cnt_card");
        map.put("3个月内申请人在多个平台申请借款_大型消费金融公司", "td_3m_tot_cnt_bigcons");
        map.put("3个月内申请人在多个平台申请借款_小额贷款公司", "td_3m_tot_cnt_minloan");
        map.put("3个月内申请人在多个平台申请借款_理财机构", "td_3m_tot_cnt_fin");
        map.put("3个月内申请人在多个平台申请借款_第三方服务商", "td_3m_tot_cnt_third");
        map.put("3个月内申请人在多个平台申请借款_财产保险", "td_3m_tot_cnt_insure");
        map.put("3个月内申请人在多个平台申请借款_银行消费金融公司", "td_3m_tot_cnt_bank_cons");
        map.put("6个月内申请人在多个平台申请借款_借款人手机个数", "td_6m_ph_cnt");
        map.put("6个月内申请人在多个平台申请借款_借款人身份证个数", "td_6m_id_cnt");
        map.put("6个月内申请人在多个平台申请借款_个数", "td_6m_tot_cnt");
        map.put("6个月内申请人在多个平台申请借款_P2P网贷", "td_6m_tot_cnt_p2p");
        map.put("6个月内申请人在多个平台申请借款_一般消费分期平台", "td_6m_tot_cnt_cons");
        map.put("6个月内申请人在多个平台申请借款_互联网金融门户", "td_6m_tot_cnt_interloan");
        map.put("6个月内申请人在多个平台申请借款_信用卡中心", "td_6m_tot_cnt_card");
        map.put("6个月内申请人在多个平台申请借款_大型消费金融公司", "td_6m_tot_cnt_bigcons");
        map.put("6个月内申请人在多个平台申请借款_小额贷款公司", "td_6m_tot_cnt_minloan");
        map.put("6个月内申请人在多个平台申请借款_理财机构", "td_6m_tot_cnt_fin");
        map.put("6个月内申请人在多个平台申请借款_第三方服务商", "td_6m_tot_cnt_third");
        map.put("6个月内申请人在多个平台申请借款_财产保险", "td_6m_tot_cnt_insure");
        map.put("6个月内申请人在多个平台申请借款_银行消费金融公司", "td_6m_tot_cnt_bank_cons");
        map.put("12个月内申请人在多个平台申请借款_借款人手机个数", "td_12m_ph_cnt");
        map.put("12个月内申请人在多个平台申请借款_借款人身份证个数", "td_12m_id_cnt");
        map.put("12个月内申请人在多个平台申请借款_个数", "td_12m_tot_cnt");
        map.put("12个月内申请人在多个平台申请借款_P2P网贷", "td_12m_tot_cnt_p2p");
        map.put("12个月内申请人在多个平台申请借款_一般消费分期平台", "td_12m_tot_cnt_cons");
        map.put("12个月内申请人在多个平台申请借款_互联网金融门户", "td_12m_tot_cnt_interloan");
        map.put("12个月内申请人在多个平台申请借款_信用卡中心", "td_12m_tot_cnt_card");
        map.put("12个月内申请人在多个平台申请借款_大型消费金融公司", "td_12m_tot_cnt_bigcons");
        map.put("12个月内申请人在多个平台申请借款_小额贷款公司", "td_12m_tot_cnt_minloan");
        map.put("12个月内申请人在多个平台申请借款_理财机构", "td_12m_tot_cnt_fin");
        map.put("12个月内申请人在多个平台申请借款_第三方服务商", "td_12m_tot_cnt_third");
        map.put("12个月内申请人在多个平台申请借款_财产保险", "td_12m_tot_cnt_insure");
        map.put("12个月内申请人在多个平台申请借款_银行消费金融公司", "td_12m_tot_cnt_bank_cons");
        map.put("18个月内申请人在多个平台申请借款_借款人手机个数", "td_18m_ph_cnt");
        map.put("18个月内申请人在多个平台申请借款_借款人身份证个数", "td_18m_id_cnt");
        map.put("18个月内申请人在多个平台申请借款_个数", "td_18m_tot_cnt");
        map.put("18个月内申请人在多个平台申请借款_P2P网贷", "td_18m_tot_cnt_p2p");
        map.put("18个月内申请人在多个平台申请借款_一般消费分期平台", "td_18m_tot_cnt_cons");
        map.put("18个月内申请人在多个平台申请借款_互联网金融门户", "td_18m_tot_cnt_interloan");
        map.put("18个月内申请人在多个平台申请借款_信用卡中心", "td_18m_tot_cnt_card");
        map.put("18个月内申请人在多个平台申请借款_大型消费金融公司", "td_18m_tot_cnt_bigcons");
        map.put("18个月内申请人在多个平台申请借款_小额贷款公司", "td_18m_tot_cnt_minloan");
        map.put("18个月内申请人在多个平台申请借款_理财机构", "td_18m_tot_cnt_fin");
        map.put("18个月内申请人在多个平台申请借款_第三方服务商", "td_18m_tot_cnt_third");
        map.put("18个月内申请人在多个平台申请借款_财产保险", "td_18m_tot_cnt_insure");
        map.put("18个月内申请人在多个平台申请借款_银行消费金融公司", "td_18m_tot_cnt_bank_cons");
        map.put("24个月内申请人在多个平台申请借款_借款人手机个数", "td_24m_ph_cnt");
        map.put("24个月内申请人在多个平台申请借款_借款人身份证个数", "td_24m_id_cnt");
        map.put("24个月内申请人在多个平台申请借款_个数", "td_24m_tot_cnt");
        map.put("24个月内申请人在多个平台申请借款_P2P网贷", "td_24m_tot_cnt_p2p");
        map.put("24个月内申请人在多个平台申请借款_一般消费分期平台", "td_24m_tot_cnt_cons");
        map.put("24个月内申请人在多个平台申请借款_互联网金融门户", "td_24m_tot_cnt_interloan");
        map.put("24个月内申请人在多个平台申请借款_信用卡中心", "td_24m_tot_cnt_card");
        map.put("24个月内申请人在多个平台申请借款_大型消费金融公司", "td_24m_tot_cnt_bigcons");
        map.put("24个月内申请人在多个平台申请借款_小额贷款公司", "td_24m_tot_cnt_minloan");
        map.put("24个月内申请人在多个平台申请借款_理财机构", "td_24m_tot_cnt_fin");
        map.put("24个月内申请人在多个平台申请借款_第三方服务商", "td_24m_tot_cnt_third");
        map.put("24个月内申请人在多个平台申请借款_财产保险", "td_24m_tot_cnt_insure");
        map.put("24个月内申请人在多个平台申请借款_银行消费金融公司", "td_24m_tot_cnt_bank_cons");
        map.put("60个月以上申请人在多个平台申请借款_借款人手机个数", "td_60m_ph_cnt");
        map.put("60个月以上申请人在多个平台申请借款_借款人身份证个数", "td_60m_id_cnt");
        map.put("60个月以上申请人在多个平台申请借款_个数", "td_60m_tot_cnt");
        map.put("60个月以上申请人在多个平台申请借款_P2P网贷", "td_60m_tot_cnt_p2p");
        map.put("60个月以上申请人在多个平台申请借款_一般消费分期平台", "td_60m_tot_cnt_cons");
        map.put("60个月以上申请人在多个平台申请借款_互联网金融门户", "td_60m_tot_cnt_interloan");
        map.put("60个月以上申请人在多个平台申请借款_信用卡中心", "td_60m_tot_cnt_card");
        map.put("60个月以上申请人在多个平台申请借款_大型消费金融公司", "td_60m_tot_cnt_bigcons");
        map.put("60个月以上申请人在多个平台申请借款_小额贷款公司", "td_60m_tot_cnt_minloan");
        map.put("60个月以上申请人在多个平台申请借款_理财机构", "td_60m_tot_cnt_fin");
        map.put("60个月以上申请人在多个平台申请借款_第三方服务商", "td_60m_tot_cnt_third");
        map.put("60个月以上申请人在多个平台申请借款_财产保险", "td_60m_tot_cnt_insure");
        map.put("60个月以上申请人在多个平台申请借款_银行消费金融公司", "td_60m_tot_cnt_bank_cons");
    }

    public static void main(String[] args) {
    }
}