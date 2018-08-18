package com.shazhx.juxinli.response;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立Data返回�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiDataResponse {

	/**
	 * 查询记录对象
	 */
	private JuXinLiQueryResposne juXinLiQueryResposne;

	/**
	 * 数据源绑定数�?
	 */
	private List<JuXinLiDataSourceResponse> dataSourceResponse;

	/**
	 * 行为�?�?
	 */
	private List<JuXinLiBehaviorCheckResponse> behaviorCheckResponse;
	private List<JuXinLiBehaviorCheckResponse4_2> behaviorCheckResponse4_2;

	/**
	 * 信息核对
	 */
	private List<JuXinLiBehaviorCheckResponse> applicationCheckResponse;
	private List<JuXinLiApplicationCheckResponse4_2> applicationCheckResponse4_2;
	
	/**
	 * 用户信息�?测（user_info_check�?
	 */
	private JuXinLiUserInfoCheckResponse userInfoResponse;
	
	/**
	 * 出行分析（trip_info�?
	 */
	private List<JuXinLiTripInfoResponse> tripInfoResponse;

	/**
	 * 联系人信�?
	 */
	public List<JuXinLiContactListResponse> contactListResponse;

	/**
	 * 近期�?求设�?
	 */
	public List<JuXinLiRecentNeedResponse> recentNeedResponse;

	/**
	 * 通话行为分析
	 */
	public List<JuXinLiCellBehaviorResponse> cellBehaviorResponse;

	/**
	 * 联系人区域汇�?
	 */
	private List<JuXinLiContactRegionResponse> contactRegionResponse;

	/**
	 * 联系人信�?
	 */
	private List<JuXinLiCollectionContacResponse> collectionContacResponse;

	/**
	 * 收货地址信息
	 */
	private List<JuXinLiDeliverAddressResponse> deliverAddressResponse;

	/**
	 * 电商月消�?
	 */
	private List<JuXinLiEbusinessExpenseResponse> ebusinessExpense;
	private List<JuXinLiEbusinessExpenseResponse4_2> ebusinessExpense4_2;

	/**
	 * 运营商明数据
	 */
	private List<JuXinLiTransactionsResponse> transactions;

	/**
	 * 电商数据
	 */
	private List<JuXinLiTransactionsEBResponse> eb_transactions;
}
