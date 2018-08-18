package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 通话行为检测
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_behavior_lib")
public class JuXinLiBehaviorInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String bill_no;// 订单号
	private String cell_phone_num;// 手机号码
	private String cell_operator;// 手机运营商
	private String cell_loc;// 手机归属地
	private String cell_mth;// 月份
	private String call_in_time;// 月被叫通话时间（分钟）
	private String call_out_time;// 月主叫通话时间（分钟）
	private String sms_cnt;// 月短信条数
	private String net_flow;// 月流量（MB）
	private String total_amount;
}
