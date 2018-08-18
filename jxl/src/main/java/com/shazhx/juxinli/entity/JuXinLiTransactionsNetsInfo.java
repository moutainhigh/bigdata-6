package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 运营商上网信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_nets_lib")
public class JuXinLiTransactionsNetsInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String bill_no;// 标示订单号
	private String nupdate_time;// 更新时间
	private String cell_phone;// 本机号码
	private String place;// 上网地区
	private String net_type;// 网络类型
	private String start_time;// 开始上网时间
	private String subflow;// 子流量
	private String use_time;// 网络使用时长

}
