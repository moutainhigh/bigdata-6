package com.mobanker.udf;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Str_to_Date extends UDF {

    public Date evaluate(String str) {
        SimpleDateFormat foramat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date rsDate = null;
        try {
            rsDate = foramat.parse(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rsDate;

    }
}