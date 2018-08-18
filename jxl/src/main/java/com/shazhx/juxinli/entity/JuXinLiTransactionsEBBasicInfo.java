package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 电商账户信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_eb_basic_lib")
public class JuXinLiTransactionsEBBasicInfo extends BaseEntity{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String billNo;// 标示订单号
	private String bupdateTime;// 更新时间
	private String email;// email地址
	private String cellPhone;// 账户手机号码
	private String securityLevel;// 安全级别
	private String level;// 会员级别
	private String isValidateRealName;// 是否实名
	private String nickname;// 账户昵称
	private String registerDate;// 注册时间
	private String realName;// 真实姓名




}
