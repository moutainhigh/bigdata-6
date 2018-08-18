package com.shazhx.juxinli.response;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.shazhx.juxinli.entity.JuXinLiTransactionsEBDatasourceInfo;

/**
 * 聚信立电商数�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsEBResponse {

	private String datasource;// 数据�?
	private List<JuXinLiTransactionsEBTransactionsResponse> transactions;// 电商交易异常
	private List<JuXinLiTransactionsEBAddressResponse> address;// 电商地址信息
	private JuXinLiTransactionsEBBasicResponse basic;// 电商账户信息
	
	private JuXinLiTransactionsEBDatasourceInfo info;
}
