package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立数据查询S
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_query_lib")
public class JuXinLiQueryInfo extends BaseEntity {

	/**
	 * Version
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 订单号
	 */
	private String billNo;

	/**
	 * 用户姓名
	 */
	private String name;

	/**
	 * 身份证号码
	 */
	private String idCard;

	/**
	 * 手机号码
	 */
	private String phone;

	/**
	 * 登录Token
	 */
	private String channel;

}
