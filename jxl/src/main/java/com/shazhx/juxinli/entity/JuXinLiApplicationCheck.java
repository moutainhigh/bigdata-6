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
@Table(name = "t_juxinli_application_check_lib")
public class JuXinLiApplicationCheck extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String bill_no;// 订单号
	private String app_point;
	private String check_points;
	private String key_value;
}
