package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立电商月消费
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_ebusiness_expense_lib")
public class JuXinLiEbusinessExpenseInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String billNo;// 标示订单号
	private String transMth;// 汇总月份
	private String ownerAmount;// 本人购物金额
	private String ownerCount;// 本人购物次数
	private String familyAmount;// 本人+同住人购物金额
	private String familyCount;// 本人+同住人购物次数
	private String othersAmount;// 非本人和家庭购物的购物金额
	private String othersCount;// 非本人和家庭购物的购物次数
	private String allAmount;// 总购物金额
	private String allCount;// 总购物次数
}
