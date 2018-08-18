package com.shazhx.juxinli.response;

import java.util.Map;

import com.shazhx.juxinli.entity.JuXinLiApplicationCheck;
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
public class JuXinLiApplicationCheckResponse4_2 {
	private String app_point;
	private Map<String,Object> check_points;
	private String key_value;
	
	private JuXinLiApplicationCheck info;
}
