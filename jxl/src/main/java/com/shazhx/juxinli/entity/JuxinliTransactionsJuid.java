package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @description: 运营商账单信息
 * @author: zhuosh
 * @DATE: 2016/7/25
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_juid")
public class JuxinliTransactionsJuid extends BaseEntity{

    /***标示订单号*/
    private String bill_no;

    /***更新记录的时间*/
    private String cupdate_time;

    /***本机手机号码*/
    private String cell_phone;

    /***账单周期*/
    private String bill_cycle;

    /***应付金额*/
    private String pay_amt;

    /***套餐金额*/
    private String plan_amt;

    /***账单总金额*/
    private String total_amt;


}
