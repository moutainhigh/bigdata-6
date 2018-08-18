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
@Table(name = "t_juxinli_trip_info_lib")
public class JuXinLiTripInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String bill_no;// 订单号
	private String trip_dest;
	private String trip_start_time;
	private String trip_end_time;
	private String trip_leave;
	private String trip_type;
}
