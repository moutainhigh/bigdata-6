package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiContactRegionInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立联系人区域汇�??
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiContactRegionResponse {
	private String region_loc;// 联系人的号码归属�?
	private String region_uniq_num_cnt;// 去重后的联系人号码数�?
	private String region_call_in_cnt;// 电话呼入次数
	private String region_call_out_cnt;// 电话呼出次数
	private String region_call_in_time;// 电话呼入总时间（秒）
	private String region_call_out_time;// 电话呼出总时间（秒）
	private String region_avg_call_in_time;// 平均电话呼入时间（秒�?
	private String region_avg_call_out_time;// 平均电话呼出时间（秒�?
	private String region_call_in_cnt_pct;// 电话呼入次数百分�?
	private String region_call_out_cnt_pct;// 电话呼出次数百分�?
	private String region_call_in_time_pct;// 电话呼入时间百分�?
	private String region_call_out_time_pct;// 电话呼出时间百分�?
	private JuXinLiContactRegionInfo info;


	public JuXinLiContactRegionInfo convertTocContactRegion(JuXinLiContactRegionResponse juXinLiContactRegionResponse){
		JuXinLiContactRegionInfo t = new JuXinLiContactRegionInfo();
		if(juXinLiContactRegionResponse != null){
			t.setRegionLoc(juXinLiContactRegionResponse.getRegion_loc());
			t.setRegionUniqNumCnt(juXinLiContactRegionResponse.getRegion_uniq_num_cnt());
			t.setRegionCallInCnt(juXinLiContactRegionResponse.getRegion_call_in_cnt());
			t.setRegionCallOutCnt(juXinLiContactRegionResponse.getRegion_call_out_cnt());
			t.setRegionCallInTime(juXinLiContactRegionResponse.getRegion_call_in_time());
			t.setRegionCallOutTime(juXinLiContactRegionResponse.getRegion_call_out_time());
			t.setRegionAvgCallOutTime(juXinLiContactRegionResponse.getRegion_avg_call_out_time());
			t.setRegionAvgCallInTime(juXinLiContactRegionResponse.getRegion_avg_call_in_time());
			t.setRegionCallInCntPct(juXinLiContactRegionResponse.getRegion_call_in_cnt_pct());
			t.setRegionCallOutCntPct(juXinLiContactRegionResponse.getRegion_call_out_cnt_pct());
			t.setRegionCallInTimePct(juXinLiContactRegionResponse.getRegion_call_in_time_pct());
			t.setRegionCallOutTimePct(juXinLiContactRegionResponse.getRegion_call_out_time_pct());
		}
		return t;
	}

}
