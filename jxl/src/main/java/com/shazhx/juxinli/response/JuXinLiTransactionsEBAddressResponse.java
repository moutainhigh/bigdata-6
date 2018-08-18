package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.shazhx.juxinli.entity.JuXinLiTransactionsEBAddressInfo;

/**
 * 电商地址信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsEBAddressResponse {
	private String update_time;// 更新时间
	private String receiver_addr;// 收货地址
	private String province;// 收货省份
	private String city;// 收货城市
	private String receiver_name;// 收货�?
	private String receiver_phone;// 收货电话
	private String is_default_address;// 是否为无效地�?

	private JuXinLiTransactionsEBAddressInfo info;

	public JuXinLiTransactionsEBAddressInfo convertToEBAddress(JuXinLiTransactionsEBAddressResponse juXinLiTransactionsEBAddressResponse){
		JuXinLiTransactionsEBAddressInfo t = new JuXinLiTransactionsEBAddressInfo();
		if(juXinLiTransactionsEBAddressResponse != null){
			t.setAupdateTime(juXinLiTransactionsEBAddressResponse.getUpdate_time());
			t.setReceiverAddr(juXinLiTransactionsEBAddressResponse.getReceiver_addr());
			t.setProvince(juXinLiTransactionsEBAddressResponse.getProvince());
			t.setCity(juXinLiTransactionsEBAddressResponse.getCity());
			t.setReceiverName(juXinLiTransactionsEBAddressResponse.getReceiver_name());
			t.setReceiverPhone(juXinLiTransactionsEBAddressResponse.getReceiver_phone());
			t.setIsDefaultAddress(juXinLiTransactionsEBAddressResponse.getIs_default_address());
		}
		return t;
	}
}
