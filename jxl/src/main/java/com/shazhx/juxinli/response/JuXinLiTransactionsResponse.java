package com.shazhx.juxinli.response;

import java.util.List;

import com.shazhx.juxinli.entity.JuXinLiTransactionsDatasourceInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立运营商明数�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsResponse {
	private String datasource;// 数据�?
	private List<JuXinLiTransactionsCallsResponse> calls;// 运营商明细信�?
	private JuXinLiTransactionsBasicResponse basic;// 运营商基本信�?
	private List<JuXinLiTransactionsNetsResponse> nets;// 运营商上网信�?
	private List<JuXinLiTransactionsSmsesResponse> smses;// 运营商短信信�?

	/***运营商账单信�?*/
	private List<JuxinliTransactionsJuidResponse> transactions;
	
	private JuXinLiTransactionsDatasourceInfo info;
}
