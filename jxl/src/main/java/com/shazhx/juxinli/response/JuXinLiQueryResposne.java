package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiQueryInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立Query返回�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiQueryResposne {

	/**
	 * 用户姓名
	 */
	private String name;

	/**
	 * 身份证号�?
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
	
	private JuXinLiQueryInfo info;

}
