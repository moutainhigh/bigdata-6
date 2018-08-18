package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.shazhx.juxinli.entity.JuXinLiTripInfo;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTripInfoResponse {
	private String trip_dest;
	private String trip_start_time;
	private String trip_end_time;
	private String trip_leave;
	private String trip_type;
	
	private JuXinLiTripInfo info;
}
