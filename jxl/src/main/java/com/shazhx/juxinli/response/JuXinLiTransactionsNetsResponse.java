package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiTransactionsNetsInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立运营商基本信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsNetsResponse {
	private String update_time;// 更新时间
	private String cell_phone;// 本机号码
	private String place;// 上网地区
	private String net_type;// 网络类型
	private String start_time;// �?始上网时�?
	private String subflow;// 子流�?
	private String use_time;// 网络使用时长
	
	private JuXinLiTransactionsNetsInfo info;
}
