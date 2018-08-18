package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.shazhx.juxinli.entity.JuXinLiCheckSearchInfo;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiCheckSearchInfoResponse {
	private String searched_org_cnt;
	private String searched_org_type;
	private String idcard_with_other_names;
	private String idcard_with_other_phones;
	private String phone_with_other_names;
	private String phone_with_other_idcards;
	private String register_org_cnt;
	private String register_org_type;
	private String arised_open_web;
	
	private JuXinLiCheckSearchInfo info;
}
