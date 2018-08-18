package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuxinliTransactionsJuid;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @description: 运营商账单信�?
 * @author: zhuosh
 * @DATE: 2016/7/26
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuxinliTransactionsJuidResponse {

    /***更新记录的时�?*/
    private String update_time;

    /***本机手机号码*/
    private String cell_phone;

    /***账单周期*/
    private String bill_cycle;

    /***应付金额*/
    private String pay_amt;

    /***套餐金额*/
    private String plan_amt;

    /***账单总金�?*/
    private String total_amt;
    
    private JuxinliTransactionsJuid info;

}
