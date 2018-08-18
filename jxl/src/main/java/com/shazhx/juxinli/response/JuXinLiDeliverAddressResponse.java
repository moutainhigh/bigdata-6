package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

import com.shazhx.juxinli.entity.JuXinLiDeliverAddressInfo;

/**
 * 聚信立�?�货地址列表
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiDeliverAddressResponse {
	private String address;// 收货地址
	private String lng;// 经度
	private String lat;// 纬度
	private String predict_addr_type;// 地址类型
	private String begin_date;// �?始�?�货时间
	private String end_date;// 结束送货时间
	private String total_amount;// 总�?�货金额
	private String total_count;// 总�?�货次数
	List<JuXinLiReceiverResponse> receiver;// 收件人信�?
	
	private JuXinLiDeliverAddressInfo info;
}
