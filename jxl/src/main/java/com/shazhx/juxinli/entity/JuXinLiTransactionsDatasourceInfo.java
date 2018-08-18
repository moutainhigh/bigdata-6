package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立运营商明细信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_datasource_lib")
public class JuXinLiTransactionsDatasourceInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;// 标示订单号
	private String transactionsBillNo;// 信息单号
	private String datasource;// 数据源

	/***是否解析了运营商账单信息 1 解析了 0没有解析（这个字段是遗留下来导致要加的）*/
	private Integer isParseJuid;
}
