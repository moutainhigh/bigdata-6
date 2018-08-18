package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiTransactionsEBTransactionsInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 电商交易明细
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsEBTransactionsResponse {
	private String update_time;// 更新时间
	private String trans_time;// 交易时间
	private String total_price;// 交易总额
	private String receiver_addr;// 收货地址
	private String receiver_name;// 收货�?
	private String receiver_cell_phone;// 收货电话
	private String payment_type;// 付款方式
	private String product_price;// 商品单价
	private String product_cnt;// 商品数量
	private String product_name;// 商品名称
	private String delivery_type;// 快�?�方�?
	private String delivery_fee;// 快�?�费�?
	private String is_success;// 数据获取是否成功
	
	private JuXinLiTransactionsEBTransactionsInfo info;


	public JuXinLiTransactionsEBTransactionsInfo convertToEBTransactionsInfo(JuXinLiTransactionsEBTransactionsResponse ebTransactionsResponse){
		JuXinLiTransactionsEBTransactionsInfo t = new JuXinLiTransactionsEBTransactionsInfo();
		if(ebTransactionsResponse != null){
			t.setTupdateTime(ebTransactionsResponse.getUpdate_time());
			t.setTransTime(ebTransactionsResponse.getTrans_time());
			t.setTotalPrice(ebTransactionsResponse.getTotal_price());
			t.setReceiverAddr(ebTransactionsResponse.getReceiver_addr());
			t.setReceiverName(ebTransactionsResponse.getReceiver_name());
			t.setReceiverCellPhone(ebTransactionsResponse.getReceiver_cell_phone());
			t.setPaymentType(ebTransactionsResponse.getPayment_type());
			t.setProductPrice(ebTransactionsResponse.getProduct_price());
			t.setProductCnt(ebTransactionsResponse.getProduct_cnt());
			t.setProductName(ebTransactionsResponse.getProduct_name());
			t.setDeliveryType(ebTransactionsResponse.getDelivery_type());
			t.setDeliveryFee(ebTransactionsResponse.getDelivery_fee());
			t.setIsSuccess(ebTransactionsResponse.getIs_success());
		}
		return t;
	}

}
