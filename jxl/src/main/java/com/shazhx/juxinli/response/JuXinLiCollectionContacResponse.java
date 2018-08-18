package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiCollectionContacInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * 聚信立联系人信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiCollectionContacResponse {
	private String contact_name;// 联系人姓�?
	private String begin_date;// 和联系人�?早联系的时间
	private String end_date;// 和联系人�?晚联系的时间
	private String total_count;// 申请人为该联系人购货总次�?
	private String total_amount;// 申请人为该联系人购货总金�?
	private List<JuXinLiCallResponse> contact_details;// 联系详情
	
	private JuXinLiCollectionContacInfo info;
}
