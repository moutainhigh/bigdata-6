package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立联系人信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_collection_contact_lib")
public class JuXinLiCollectionContacInfo extends BaseEntity{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 标示订单号
	private String call_bill_no;// 呼叫记录订单号
	private String contact_name;// 联系人姓名
	private String begin_date;// 和联系人最早联系的时间
	private String end_date;// 和联系人最晚联系的时间
	private String total_count;// 申请人为该联系人购货总次数
	private String total_amount;// 申请人为该联系人购货总金额
}
