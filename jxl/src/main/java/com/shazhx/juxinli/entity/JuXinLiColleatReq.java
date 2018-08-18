/**
 * 
 */
package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @类说明:聚信立日征信统计
 * @类名:JuXinLiColleatReq.java
 * @作者:xiaoshijie
 * @创建日期:2016年2月26日
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_colleatreq")
public class JuXinLiColleatReq extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7876930006263624456L;

	// 用户的手机号
	private String mobile;

	// 用户的身份证号
	private String idCard;

	// 用户的姓名
	private String name;

	// 统计月份
	private String stasticMonth;

}
