package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_check_search_info_lib")
public class JuXinLiCheckSearchInfo extends BaseEntity {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String billNo;// 订单类型
	private String searched_org_cnt;
	private String searched_org_type;
	private String idcard_with_other_names;
	private String idcard_with_other_phones;
	private String phone_with_other_names;
	private String phone_with_other_idcards;
	private String register_org_cnt;
	private String register_org_type;
	private String arised_open_web;
}
