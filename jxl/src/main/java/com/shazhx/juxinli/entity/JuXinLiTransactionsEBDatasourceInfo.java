package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立电商数据
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_eb_datasource_lib")
public class JuXinLiTransactionsEBDatasourceInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;// 标示订单号
	private String transactionsBillNo;// 信息单号
	private String datasource;// 数据源

}
