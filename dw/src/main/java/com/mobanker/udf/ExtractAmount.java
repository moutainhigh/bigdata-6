package com.mobanker.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liuxinyuan on 2017/12/26.
 * [[1000.0,\"人民币\"],true,\"1000.00元\"]
 */
public class ExtractAmount extends UDF {
    public Double evaluate(String str) {
        if (str == null || str.length() <= 3) {
            return null;
        }
        try {
            return Double.valueOf(str.substring(2, str.indexOf(',')));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void main(String[] args){
        ExtractAmount extractAmount = new ExtractAmount();
        System.out.println(extractAmount.evaluate("[[1000.0,\\\"人民币\\\"],true,\\\"1000.00元\\\"]"));
    }
}
