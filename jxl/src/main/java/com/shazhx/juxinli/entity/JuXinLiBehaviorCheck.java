package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_behavior_check_lib")
public class JuXinLiBehaviorCheck extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;// 订单类型
	private String check_point;// 检查项目
	private String check_point_cn;// 检查项目
	private String result;// 检查结果
	private String evidence;// 证据
	private String score;// 0:无数据, 1:通过, 2:不通过

}
