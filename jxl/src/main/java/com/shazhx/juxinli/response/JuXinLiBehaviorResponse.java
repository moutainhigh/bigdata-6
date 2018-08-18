package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiBehaviorInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立�?�话行为分析
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiBehaviorResponse {
	private String cell_phone_num;// 手机号码
	private String cell_operator;// 手机运营�?
	private String cell_loc;// 手机归属�?
	private String cell_mth;// 月份
	private String call_in_time;// 月被叫�?�话时间（分钟）
	private String call_out_time;// 月主叫�?�话时间（分钟）
	private String sms_cnt;// 月短信条�?
	private String net_flow;// 月流量（MB�?
	private String total_amount;
	
	private JuXinLiBehaviorInfo info;
}
