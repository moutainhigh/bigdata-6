package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 近期需求主表
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_recent_need_lib")
public class JuXinLiRecentNeedInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 订单号
	private String demands_bill_no;// 详情表订单号
	private String req_type;// 需求类型
	private String call_out_cnt;// 对该需求的主叫总次数
	private String call_in_cnt;// 对该需求的被叫总次数
	private String call_out_time;// 对该需求的主叫总时长（秒）
	private String call_in_time;// 对该需求的被叫总时长（秒）
	private String req_mth;// 需求发生的月份
}
