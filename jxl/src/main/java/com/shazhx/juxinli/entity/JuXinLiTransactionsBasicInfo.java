package com.shazhx.juxinli.entity;

import javax.persistence.Table;

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
@Table(name = "t_juxinli_transactions_basic_lib")
public class JuXinLiTransactionsBasicInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;// 标示订单号
	private String bupdateTime;// 更新时间
	private String cellPhone;// 本机号码
	private String idcard;// 证件号
	private String regTime;// 登记时间
	private String realName;// 姓名

}
