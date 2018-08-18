package com.shazhx.juxinli.entity;

import java.util.Date;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立token维护表
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_token_valid_lib")
public class JuXinLiTokenInfo extends BaseEntity {

	/**
	 * Version
	 */
	private static final long serialVersionUID = 1L;

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
	private String mobile;

	/**
	 * 登录Token
	 */
	private String token;

	/**
	 * 有效时间
	 */
	private Date validTime;

}
