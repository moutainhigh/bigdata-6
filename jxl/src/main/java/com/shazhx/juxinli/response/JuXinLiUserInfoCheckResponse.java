package com.shazhx.juxinli.response;

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
public class JuXinLiUserInfoCheckResponse {
	private JuXinLiCheckSearchInfoResponse checkSearchInfo;
	private JuXinLiCheckBlackInfoResponse checkBlackInfo;
}
