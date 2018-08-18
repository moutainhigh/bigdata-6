package com.mobanker.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.StringTokenizer;

public class PhoneClean extends UDF {

    public  String evaluate(String str) {
        if (str == null) {
            return null;
        }
        String parserStr = parserStr(str);

        if (StringUtils.isBlank(parserStr)) {
            return null;
        }

        int length = parserStr.length();
        if (length >= 11) {
            String substring = parserStr.substring((length - 11));
            if (substring.startsWith("13") || substring.startsWith("14")
                    || substring.startsWith("15") || substring.startsWith("17") || substring.startsWith("18")) {
                return substring;
            } else {
                return parserStr.substring(0, length >= 15 ? 15 : length);
            }
        }
        return parserStr;
    }

    /**
     * 解析电话号码 去掉非数字字符
     *
     * @param str
     * @return
     */
    public  String parserStr(String str) {

        StringBuffer result = new StringBuffer();
        String[] strs = str.split("\\D");
        for (int i = 0; i < strs.length; i++) {
            if (StringUtils.isNotBlank(strs[i])) {
                result.append(strs[i]);
            }
        }
        return result.toString();
    }


}
