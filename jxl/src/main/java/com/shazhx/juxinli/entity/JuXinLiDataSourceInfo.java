package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立绑定数据源
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_data_source_lib")
public class JuXinLiDataSourceInfo extends BaseEntity {

	/**
	 * Version
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 订单号
	 */
	private String billNo;

	private String skey;// 数据源标识
	private String name;// 数据源名称
	private String account;// 账号名称
	private String categoryName;// 数据类型
	private String categoryValue;// 数据类型名称
	private String status;// 数据有效性
	private String reliability;// 数据可靠性
	private String bindingTime;// 绑定时间
}
