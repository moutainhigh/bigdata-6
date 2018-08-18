package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 需求明细
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_demands_info_lib")
public class JuXinLiDomandsInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 订单号
	private String demands_name;// 需求名称
	private String demands_call_out_cnt;// 对该需求的主叫总次数
	private String demands_call_in_cnt;// 对该需求的被叫总次数
	private String demands_call_out_time;// 对该需求的主叫总时长（秒）
	private String demands_call_in_time;// 对该需求的被叫总时长（秒）
}
