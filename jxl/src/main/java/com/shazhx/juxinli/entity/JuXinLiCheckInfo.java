package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立检测核对
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_behavior_application_check_lib")
public class JuXinLiCheckInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;// 订单类型
	private String channel;// 数据类型：application_check 行为检测：behavior_check
	private String category;// 检查点类别
	private String checkPoint;// 检查项目
	private String result;// 检查结果
	private String evidence;// 证据

}
