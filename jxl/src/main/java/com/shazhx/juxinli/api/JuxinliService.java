package com.shazhx.juxinli.api;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.shazhx.juxinli.enumerate.JuXinLiQueryChannel;
import net.sf.json.JSONArray;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import com.shazhx.juxinli.entity.AccessReportRequset;
import com.shazhx.juxinli.entity.JuXinLiApplicationCheck;
import com.shazhx.juxinli.entity.JuXinLiBehaviorCheck;
import com.shazhx.juxinli.entity.JuXinLiBehaviorInfo;
import com.shazhx.juxinli.entity.JuXinLiCallInfo;
import com.shazhx.juxinli.entity.JuXinLiCellBehaviorInfo;
import com.shazhx.juxinli.entity.JuXinLiCheckBlackInfo;
import com.shazhx.juxinli.entity.JuXinLiCheckInfo;
import com.shazhx.juxinli.entity.JuXinLiCheckSearchInfo;
import com.shazhx.juxinli.entity.JuXinLiCollectionContacInfo;
import com.shazhx.juxinli.entity.JuXinLiContactListInfo;
import com.shazhx.juxinli.entity.JuXinLiContactRegionInfo;
import com.shazhx.juxinli.entity.JuXinLiDataSourceInfo;
import com.shazhx.juxinli.entity.JuXinLiDeliverAddressInfo;
import com.shazhx.juxinli.entity.JuXinLiDomandsInfo;
import com.shazhx.juxinli.entity.JuXinLiEbusinessExpenseInfo;
import com.shazhx.juxinli.entity.JuXinLiEbusinessExpenseInfo4_2;
import com.shazhx.juxinli.entity.JuXinLiQueryInfo;
import com.shazhx.juxinli.entity.JuXinLiReceiverInfo;
import com.shazhx.juxinli.entity.JuXinLiRecentNeedInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsBasicInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsCallsInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsDatasourceInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsEBAddressInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsEBBasicInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsEBDatasourceInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsEBTransactionsInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsNetsInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsSmsesInfo;
import com.shazhx.juxinli.entity.JuXinLiTripInfo;
import com.shazhx.juxinli.entity.JuxinliTransactionsJuid;
import com.shazhx.juxinli.entity.ResponseEntity;
import com.shazhx.juxinli.response.JuXinLiApplicationCheckResponse4_2;
import com.shazhx.juxinli.response.JuXinLiBehaviorCheckResponse;
import com.shazhx.juxinli.response.JuXinLiBehaviorCheckResponse4_2;
import com.shazhx.juxinli.response.JuXinLiBehaviorResponse;
import com.shazhx.juxinli.response.JuXinLiCallResponse;
import com.shazhx.juxinli.response.JuXinLiCellBehaviorResponse;
import com.shazhx.juxinli.response.JuXinLiCheckBlackInfoResponse;
import com.shazhx.juxinli.response.JuXinLiCheckSearchInfoResponse;
import com.shazhx.juxinli.response.JuXinLiCollectionContacResponse;
import com.shazhx.juxinli.response.JuXinLiContactListResponse;
import com.shazhx.juxinli.response.JuXinLiContactRegionResponse;
import com.shazhx.juxinli.response.JuXinLiDataResponse;
import com.shazhx.juxinli.response.JuXinLiDataSourceResponse;
import com.shazhx.juxinli.response.JuXinLiDeliverAddressResponse;
import com.shazhx.juxinli.response.JuXinLiDemandsInfoResponse;
import com.shazhx.juxinli.response.JuXinLiEbusinessExpenseResponse;
import com.shazhx.juxinli.response.JuXinLiEbusinessExpenseResponse4_2;
import com.shazhx.juxinli.response.JuXinLiQueryResposne;
import com.shazhx.juxinli.response.JuXinLiReceiverResponse;
import com.shazhx.juxinli.response.JuXinLiRecentNeedReqCallCntResponse;
import com.shazhx.juxinli.response.JuXinLiRecentNeedReqCallMinResponse;
import com.shazhx.juxinli.response.JuXinLiRecentNeedResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsBasicResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsCallsResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsEBAddressResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsEBBasicResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsEBResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsEBTransactionsResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsNetsResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsResponse;
import com.shazhx.juxinli.response.JuXinLiTransactionsSmsesResponse;
import com.shazhx.juxinli.response.JuXinLiTripInfoResponse;
import com.shazhx.juxinli.response.JuXinLiUserInfoCheckResponse;
import com.shazhx.juxinli.response.JuxinliTransactionsJuidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.mobanker.common.utils.BillNoUtils;

public class JuxinliService {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * 
	 * @param name
	 * @param idCard
	 * @param mobile
	 * @param channel
	 *            ("all", "mobile", "e_business")
	 * @param json
	 * @return
	 * @throws Exception
	 */
	public JuXinLiDataResponse parse(String name, String idCard, String mobile,
			String channel, String json, String billNo) throws Exception {
		if (StringUtils.isEmpty(name) || StringUtils.isEmpty(idCard)
				|| StringUtils.isEmpty(mobile) || StringUtils.isEmpty(name)
				|| StringUtils.isEmpty(json)) {
			throw new RuntimeException("缺少必要参数！");
		}

		JuXinLiDataResponse resposne = new JuXinLiDataResponse();
		ResponseEntity re = null;
		try {
			AccessReportRequset accessReportRequset = new AccessReportRequset();
			accessReportRequset.setName(name);
			accessReportRequset.setIdCard(idCard);
			accessReportRequset.setPhone(mobile);
			accessReportRequset.setChannel(channel);

			re = JSONObject.parseObject(json, ResponseEntity.class);
			re.setTranserialsId(billNo);
			resposne = saveJuXinLiData2DB(accessReportRequset, re);
		} catch (Exception e) {
			throw e;
		}
		return resposne;
	}

	private JuXinLiDataResponse saveJuXinLiData2DB(AccessReportRequset request,
			ResponseEntity resp) throws Exception {
		JuXinLiDataResponse resposne = new JuXinLiDataResponse();
		if (resp.getData() != null) {
			JSONObject json = JSONObject.parseObject(resp.getData().toString());
			// �ж�json��Ϣ
			if (json == null || !"true".equals(json.get("success"))) {
				logger.info("�û�������Ϣ��" + json.get("note"));
				throw new Exception();
			}
			resposne.setJuXinLiQueryResposne(setQueryData(request, resp));

			// ȫ����
			if (JuXinLiQueryChannel.ALL.toString().equals(request.getChannel())) {
				JSONObject report_data_json = json.getJSONObject("report_data");

				// �������ݴ���
				if (json.containsKey("report_data")) {
					// �жϱ���汾
					JSONObject report = report_data_json
							.getJSONObject("report");
					String version = report.getString("version");

					if ("4.2".equals(version)) {
						saveReport4_2(resposne, resp, report_data_json);
					} else {
						saveReport4_0(resposne, resp, report_data_json);
					}
				}

			} else if (JuXinLiQueryChannel.MOBILE.toString().equals(// �ƶ�����
					request.getChannel())) {

				if (json.containsKey("raw_data")) {
					JSONObject raw_data_json = json.getJSONObject("raw_data");
					JSONObject members_json = raw_data_json
							.getJSONObject("members");
					String error_code = members_json.getString("error_code");
					if ("31200".equals(error_code)) {// �������ݳɹ�

						// ��ȡ��Ӫ������
						List<JuXinLiTransactionsResponse> data = JSONObject
								.parseArray(
										members_json.getString("transactions"),
										JuXinLiTransactionsResponse.class);
						resposne.setTransactions(data);

						// ��ϵ�ͱ� �������
						for (JuXinLiTransactionsResponse juXinLiTransactionsResponse : data) {

							// �����ֻ�����Դ
							JuXinLiTransactionsDatasourceInfo t = new JuXinLiTransactionsDatasourceInfo();
							t.setBillNo(resp.getTranserialsId());
							t.setTransactionsBillNo(resp.getTranserialsId());
							t.setDatasource(juXinLiTransactionsResponse
									.getDatasource());
							t.setIsParseJuid(1);
							juXinLiTransactionsResponse.setInfo(t);
							// juXinLiTransactionsDatasourceInfoDao.insert(t);

							// ����绰��Ӫ������
							setTransactionsCalls(juXinLiTransactionsResponse,
									t.getTransactionsBillNo());

							// ������Ӫ�̻�����Ϣ
							setTransactionsBasic(juXinLiTransactionsResponse,
									t.getTransactionsBillNo());

							// ��Ӫ���˵���Ϣ
							setJuxinliTransactionsJuid(
									juXinLiTransactionsResponse,
									t.getTransactionsBillNo());

							// ��Ӫ��������Ϣ
							setTransactionsNets(juXinLiTransactionsResponse,
									t.getTransactionsBillNo());

							// ��Ӫ�̶�����Ϣ
							setTransactionsSmses(juXinLiTransactionsResponse,
									t.getTransactionsBillNo());

						}
					}
				}

			} else if (JuXinLiQueryChannel.E_BUSINESS.toString().equals(// ��������
					request.getChannel())) {

				if (json.containsKey("raw_data")) {
					JSONObject raw_data_json = json.getJSONObject("raw_data");
					JSONObject members_json = raw_data_json.getJSONObject("members");
					String error_code = members_json.getString("error_code");
					if ("31200".equals(error_code)) {// �������ݳɹ�

						// ��ȡ��Ӫ������
						List<JuXinLiTransactionsEBResponse> data = JSONObject
								.parseArray(
										members_json.getString("transactions"),
										JuXinLiTransactionsEBResponse.class);
						resposne.setEb_transactions(data);

						// ��ϵ�ͱ� �������
						for (JuXinLiTransactionsEBResponse juXinLiTransactionsEBResponse : data) {

							// �����ֻ�����Դ
							JuXinLiTransactionsEBDatasourceInfo t = new JuXinLiTransactionsEBDatasourceInfo();
							t.setBillNo(resp.getTranserialsId());
							t.setTransactionsBillNo(resp.getTranserialsId());
							t.setDatasource(juXinLiTransactionsEBResponse
									.getDatasource());
							juXinLiTransactionsEBResponse.setInfo(t);
							// juXinLiTransactionsEBDatasourceInfoDao.insert(t);

							// ������̽�����ϸ����
							setTransactionsEbs(juXinLiTransactionsEBResponse,
									t.getTransactionsBillNo());

							// ���̵�ַ��Ϣ
							setTransactionsAddress(
									juXinLiTransactionsEBResponse,
									t.getTransactionsBillNo());

							// �����˻���Ϣ
							setTransactionsBasic(juXinLiTransactionsEBResponse,
									t.getTransactionsBillNo());
						}
					}
				}
			}

			resp.setData(resposne);
		} else {
			logger.error("�û�������Ϣ, response:{}", resp);
			throw new Exception();
		}
		return resposne;
	}

	/**
	 * �����������ѯ����
	 * 
	 * @param request
	 * @param resp
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 */
	private JuXinLiQueryResposne setQueryData(AccessReportRequset request,
			ResponseEntity resp) throws IllegalAccessException,
			InvocationTargetException {
		// ��������¼����
		JuXinLiQueryInfo juXinLiQueryInfo = new JuXinLiQueryInfo();
		BeanUtils.copyProperties(juXinLiQueryInfo, request);
		// �����жϵ�ǰ����û���Ϣ�Ƿ��Ѿ�����
		// JuXinLiQueryInfo temp = juXinLiQueryInfoDao.getOne(juXinLiQueryInfo);
		// if( temp == null){
		// juXinLiQueryInfo.setBillNo(resp.getTranserialsId());
		// juXinLiQueryInfoDao.insert(juXinLiQueryInfo);
		// }else{
		// juXinLiQueryInfo = temp;
		// }

		juXinLiQueryInfo.setBillNo(resp.getTranserialsId());
		// juXinLiQueryInfoDao.insert(juXinLiQueryInfo);
		JuXinLiQueryResposne response = new JuXinLiQueryResposne();
		BeanUtils.copyProperties(response, juXinLiQueryInfo);
		response.setInfo(juXinLiQueryInfo);
		return response;
	}

	/**
	 * ���ð�����Դ
	 * 
	 * @param resp
	 * 
	 * @param report_data_json
	 */
	private List<JuXinLiDataSourceResponse> setDataSource(ResponseEntity resp,
			JSONObject report_data_json) {
		// data_source ������Դ��Ϣ
		List<JuXinLiDataSourceResponse> data = null;
		try {
			data = JSONArray.toList(
					JSONArray.fromObject(report_data_json.get("data_source")),
					JuXinLiDataSourceResponse.class);

			for (JuXinLiDataSourceResponse juXinLiDataSourceResponse : data) {
				JuXinLiDataSourceInfo t = juXinLiDataSourceResponse
						.convertToJuXinLiDataSource(juXinLiDataSourceResponse);

				t.setBillNo(resp.getTranserialsId());
				t.setSkey(juXinLiDataSourceResponse.getKey());
				juXinLiDataSourceResponse.setInfo(t);
				// juXinLiDataSourceInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setDataSource to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ��������Ϊ���
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiBehaviorCheckResponse> setBehaviorCheck(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiBehaviorCheckResponse> data = null;
		try {
			data = JSONArray
					.toList(JSONArray.fromObject(report_data_json
							.get("behavior_check")),
							JuXinLiBehaviorCheckResponse.class);

			for (JuXinLiBehaviorCheckResponse item : data) {
				JuXinLiCheckInfo t = item.convertToBehaviorCheck(item);
				// BeanUtils.copyProperties(item, t);
				t.setBillNo(resp.getTranserialsId());
				t.setChannel("behavior_check");
				item.setInfo(t);
				// juXinLiBehaviorCheckInfoDao.insert(t);

			}
		} catch (Exception ex) {
			logger.error(
					"info insert setBehaviorCheck to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ��������Ϊ���
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiBehaviorCheckResponse> setApplicationCheck(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiBehaviorCheckResponse> data = null;
		try {
			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("application_check")),
					JuXinLiBehaviorCheckResponse.class);

			for (JuXinLiBehaviorCheckResponse item : data) {
				JuXinLiCheckInfo t = item.convertToBehaviorCheck(item);
				// BeanUtils.copyProperties(item, t);
				t.setBillNo(resp.getTranserialsId());
				t.setChannel("application_check");
				item.setInfo(t);
				// juXinLiBehaviorCheckInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setApplicationCheck to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ��������ϵ������
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiContactListResponse> setContactList(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiContactListResponse> data = null;
		try {
			data = JSONArray.toList(
					JSONArray.fromObject(report_data_json.get("contact_list")),
					JuXinLiContactListResponse.class);

			for (JuXinLiContactListResponse item : data) {
				JuXinLiContactListInfo t = new JuXinLiContactListInfo();
				org.springframework.beans.BeanUtils.copyProperties(item, t);
				t.setBillNo(resp.getTranserialsId());
				item.setInfo(t);
				// juXinLiContactListInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setContactList to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ��������
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiRecentNeedResponse> setRecentNeed(ResponseEntity resp,
			JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiRecentNeedResponse> data = null;
		try {
			Map<String, Class> classMap = new HashMap<String, Class>();
			classMap.put("demands_info", JuXinLiDemandsInfoResponse.class);
			data = JSONArray.toList(
					JSONArray.fromObject(report_data_json.get("recent_need")),
					JuXinLiRecentNeedResponse.class, classMap);

			for (JuXinLiRecentNeedResponse item : data) {
				JuXinLiRecentNeedInfo t = new JuXinLiRecentNeedInfo();
				t.setBill_no(resp.getTranserialsId());
				t.setDemands_bill_no(BillNoUtils.GenerateBillNo());
				t.setReq_type(item.getReq_type());
				t.setReq_mth(item.getReq_mth());
				JuXinLiRecentNeedReqCallCntResponse cnt = item
						.getReq_call_cnt();
				t.setCall_in_cnt(cnt.getCall_in_cnt());
				t.setCall_out_cnt(cnt.getCall_out_cnt());
				JuXinLiRecentNeedReqCallMinResponse min = item
						.getReq_call_min();
				t.setCall_in_time(min.getCall_in_time());
				t.setCall_out_time(min.getCall_out_time());
				item.setInfo(t);

				for (JuXinLiDemandsInfoResponse ditem : item.getDemands_info()) {
					JuXinLiDomandsInfo dt = new JuXinLiDomandsInfo();
					org.springframework.beans.BeanUtils.copyProperties(ditem, dt);
					dt.setBill_no(t.getDemands_bill_no());
					ditem.setInfo(dt);
					// juXinLiDomandsInfoDao.insert(dt);

				}
				// juXinLiRecentNeedInfoDao.insert(t);
			}

		} catch (Exception ex) {
			logger.error(
					"info insert setContactList to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ͨ����Ϊ����
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiCellBehaviorResponse> setCellBehavior(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiCellBehaviorResponse> data = null;
		try {
			Map<String, Class> classMap = new HashMap<String, Class>();
			classMap.put("behavior", JuXinLiBehaviorResponse.class);
			data = JSONArray
					.toList(JSONArray.fromObject(report_data_json
							.get("cell_behavior")),
							JuXinLiCellBehaviorResponse.class, classMap);

			for (JuXinLiCellBehaviorResponse item : data) {
				JuXinLiCellBehaviorInfo t = new JuXinLiCellBehaviorInfo();
				t.setBill_no(resp.getTranserialsId());
				t.setPhone_num(item.getPhone_num());
				t.setBehavior_bill_no(resp.getTranserialsId());
				item.setInfo(t);

				for (JuXinLiBehaviorResponse bitem : item.getBehavior()) {
					JuXinLiBehaviorInfo bt = new JuXinLiBehaviorInfo();
					org.springframework.beans.BeanUtils.copyProperties(bitem, bt);
					bt.setBill_no(t.getBehavior_bill_no());
					bitem.setInfo(bt);
					// juXinLiBehaviorInfoDao.insert(bt);
				}
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setCellBehavior to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ��ϵ���������
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiContactRegionResponse> setContactRegion(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiContactRegionResponse> data = null;
		try {
			data = JSONArray
					.toList(JSONArray.fromObject(report_data_json
							.get("contact_region")),
							JuXinLiContactRegionResponse.class);

			for (JuXinLiContactRegionResponse item : data) {
				JuXinLiContactRegionInfo t = item.convertTocContactRegion(item);
				// BeanUtils.copyProperties(item, t);
				t.setBillNo(resp.getTranserialsId());
				item.setInfo(t);
				// juXinLiContactRegionInfoDao.insert(t);
			}

		} catch (Exception ex) {
			logger.error(
					"info insert setContactRegion to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ��ϵ����Ϣ
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiCollectionContacResponse> setCollectionContact(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiCollectionContacResponse> data = null;
		try {
			Map<String, Class> classMap = new HashMap<String, Class>();
			classMap.put("contact_details", JuXinLiCallResponse.class);
			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("collection_contact")),
					JuXinLiCollectionContacResponse.class, classMap);

			for (JuXinLiCollectionContacResponse item : data) {
				JuXinLiCollectionContacInfo t = new JuXinLiCollectionContacInfo();
				org.springframework.beans.BeanUtils.copyProperties(item, t);
				t.setBill_no(resp.getTranserialsId());
				t.setCall_bill_no(resp.getTranserialsId());
				item.setInfo(t);

				for (JuXinLiCallResponse citem : item.getContact_details()) {
					JuXinLiCallInfo ct = new JuXinLiCallInfo();
					org.springframework.beans.BeanUtils.copyProperties(citem, ct);
					ct.setBill_no(t.getCall_bill_no());
					citem.setInfo(ct);
					// juXinLiCallInfoDao.insert(ct);

				}
			}

		} catch (Exception ex) {
			logger.error(
					"info insert setCollectionContact to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * �ջ���ַ��Ϣ
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiDeliverAddressResponse> setDeliverAddress(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiDeliverAddressResponse> data = null;
		try {
			Map<String, Class> classMap = new HashMap<String, Class>();
			classMap.put("receiver", JuXinLiReceiverResponse.class);
			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("deliver_address")),
					JuXinLiDeliverAddressResponse.class, classMap);

			for (JuXinLiDeliverAddressResponse item : data) {
				JuXinLiDeliverAddressInfo t = new JuXinLiDeliverAddressInfo();
				org.springframework.beans.BeanUtils.copyProperties(item, t);
				t.setBill_no(resp.getTranserialsId());
				t.setReceiver_bill_no(resp.getTranserialsId());
				item.setInfo(t);

				for (JuXinLiReceiverResponse ritem : item.getReceiver()) {
					JuXinLiReceiverInfo rt = new JuXinLiReceiverInfo();
					org.springframework.beans.BeanUtils.copyProperties(ritem, rt);
					rt.setBill_no(t.getReceiver_bill_no());
					rt.setPhone_num_list(StringUtils.join(
							ritem.getPhone_num_list(), ','));
					ritem.setInfo(rt);
					// juXinLiReceiverInfoDao.insert(rt);

				}
				//
				// // juXinLiDeliverAddressInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setDeliverAddress to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ����������
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiEbusinessExpenseResponse> setEbusinessExpense(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiEbusinessExpenseResponse> data = null;
		try {

			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("ebusiness_expense")),
					JuXinLiEbusinessExpenseResponse.class);

			for (JuXinLiEbusinessExpenseResponse item : data) {
				JuXinLiEbusinessExpenseInfo t = new JuXinLiEbusinessExpenseInfo();
				// BeanUtils.copyProperties(item, t);

				t.setAllAmount(item.getAll_amount());
				t.setAllCount(item.getAll_count());
				t.setFamilyAmount(item.getFamily_amount());
				t.setFamilyCount(item.getFamily_count());
				t.setOthersAmount(item.getOthers_amount());
				t.setOthersCount(item.getOthers_count());
				t.setOwnerAmount(item.getOwner_amount());
				t.setOwnerCount(item.getOwner_count());
				t.setTransMth(item.getTrans_mth());
				t.setBillNo(resp.getTranserialsId());
				item.setInfo(t);
				// juXinLiEbusinessExpenseInfoDao.insert(t);
			}

		} catch (Exception ex) {
			logger.error(
					"info insert setEbusinessExpense to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ������Ӫ������
	 * 
	 * @param data
	 *            ��Ӫ������
	 * 
	 * 
	 * @param transactionsBillNo
	 *            ���ݵ���
	 * 
	 * @return
	 */
	private void setTransactionsCalls(JuXinLiTransactionsResponse data,
			String transactionsBillNo) {
		try {
			List<JuXinLiTransactionsCallsResponse> calls = data.getCalls();
			for (JuXinLiTransactionsCallsResponse juXinLiTransactionsCallsResponse : calls) {
				JuXinLiTransactionsCallsInfo t = juXinLiTransactionsCallsResponse
						.convertToTransactionsCallsInfo(juXinLiTransactionsCallsResponse);
				t.setBillNo(transactionsBillNo);
				juXinLiTransactionsCallsResponse.setInfo(t);
				// juXinLiTransactionsCallsInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setTransactionsCalls to db method, Exception ex:{}",
					ex);
		}
	}

	/**
	 * �����˻���Ϣ
	 * 
	 * @param juXinLiTransactionsEBResponse
	 * @param transactions_bill_no
	 */

	private void setTransactionsBasic(
			JuXinLiTransactionsEBResponse juXinLiTransactionsEBResponse,
			String transactions_bill_no) {
		try {
			JuXinLiTransactionsEBBasicResponse basice = juXinLiTransactionsEBResponse
					.getBasic();
			if (basice != null) {
				JuXinLiTransactionsEBBasicInfo t = basice
						.convertEBBasicInfo(basice);
				org.springframework.beans.BeanUtils.copyProperties(basice, t);
				t.setBillNo(transactions_bill_no);
				basice.setInfo(t);
				// juXinLiTransactionsEBBasicInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setTransactionsBasic to db method, Exception ex:{}",
					ex);
		}

	}

	/**
	 * ������Ӫ�̻�����Ϣ
	 * 
	 * @param data
	 *            ��Ӫ������
	 * 
	 * 
	 * @param transactionsBillNo
	 *            ���ݵ���
	 * 
	 * @return
	 */
	private void setTransactionsBasic(JuXinLiTransactionsResponse data,
			String transactionsBillNo) {
		try {
			JuXinLiTransactionsBasicResponse basic = data.getBasic();
			if (basic != null) {
				JuXinLiTransactionsBasicInfo t = basic.convert(basic);
				// BeanUtils.copyProperties(basic, t);
				t.setBillNo(transactionsBillNo);
				// t.setBupdateTime(basic.getUpdate_time());
				basic.setInfo(t);
				// juXinLiTransactionsBasicInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setTransactionsBasic to db method, Exception ex:{}",
					ex);
		}
	}

	/***
	 * ������Ӫ���˵���Ϣ
	 * 
	 * @param data
	 * @param transactionsBillNo
	 */
	private void setJuxinliTransactionsJuid(JuXinLiTransactionsResponse data,
			String transactionsBillNo) {
		try {
			List<JuxinliTransactionsJuidResponse> list = data.getTransactions();
			if (list != null && list.size() > 0) {
				JuxinliTransactionsJuid juid = null;
				for (int i = 0; i < list.size(); i++) {
					juid = new JuxinliTransactionsJuid();
					org.springframework.beans.BeanUtils.copyProperties(list.get(i), juid);
					juid.setBill_no(transactionsBillNo);
					juid.setCupdate_time(list.get(i).getUpdate_time());
					list.get(i).setInfo(juid);
					// juxinliTransactionsJuidDao.insert(juid);
				}
			}
		} catch (Exception e) {
			logger.error(
					"info insert setTransactionsBasic to db method, Exception e:{}",
					e);
		}
	}

	/**
	 * ��Ӫ��������Ϣ
	 * 
	 * @param data
	 *            ��Ӫ������
	 * 
	 * 
	 * @param transactionsBillNo
	 *            ���ݵ���
	 * 
	 * @return
	 */
	private void setTransactionsNets(JuXinLiTransactionsResponse data,
			String transactionsBillNo) {
		try {
			List<JuXinLiTransactionsNetsResponse> nets = data.getNets();
			for (JuXinLiTransactionsNetsResponse juXinLiTransactionsNetsResponse : nets) {
				JuXinLiTransactionsNetsInfo t = new JuXinLiTransactionsNetsInfo();
				org.springframework.beans.BeanUtils.copyProperties(juXinLiTransactionsNetsResponse, t);
				t.setBill_no(transactionsBillNo);
				t.setNupdate_time(juXinLiTransactionsNetsResponse
						.getUpdate_time());
				juXinLiTransactionsNetsResponse.setInfo(t);
				// juXinLiTransactionsNetsInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setTransactionsNets to db method, Exception ex:{}",
					ex);
		}
	}

	/**
	 * ��Ӫ�̶�����Ϣ
	 * 
	 * @param data
	 *            ��Ӫ������
	 * 
	 * 
	 * @param transactionsBillNo
	 *            ���ݵ���
	 * 
	 * @return
	 */
	private void setTransactionsSmses(JuXinLiTransactionsResponse data,
			String transactionsBillNo) {
		try {
			List<JuXinLiTransactionsSmsesResponse> nets = data.getSmses();
			for (JuXinLiTransactionsSmsesResponse juXinLiTransactionsSmsesResponse : nets) {
				JuXinLiTransactionsSmsesInfo t = juXinLiTransactionsSmsesResponse
						.concertToTransactionsSmses(juXinLiTransactionsSmsesResponse);
				t.setBillNo(transactionsBillNo);
				juXinLiTransactionsSmsesResponse.setInfo(t);
				// juXinLiTransactionsSmsesInfoDao.insert(t);
			}
		} catch (Exception ex) {

			logger.error(
					"info insert setTransactionsSmses to db method, Exception ex:{}",
					ex);
		}
	}

	/**
	 * ������̽�����ϸ����
	 * 
	 * @param juXinLiTransactionsEBResponse
	 * @param transactions_bill_no
	 */
	private void setTransactionsEbs(
			JuXinLiTransactionsEBResponse juXinLiTransactionsEBResponse,
			String transactions_bill_no) {
		try {
			List<JuXinLiTransactionsEBTransactionsResponse> transactions = juXinLiTransactionsEBResponse
					.getTransactions();
			for (JuXinLiTransactionsEBTransactionsResponse juXinLiTransactionsEBTransactionsResponse : transactions) {
				JuXinLiTransactionsEBTransactionsInfo t = juXinLiTransactionsEBTransactionsResponse
						.convertToEBTransactionsInfo(juXinLiTransactionsEBTransactionsResponse);
				t.setBillNo(transactions_bill_no);
				juXinLiTransactionsEBTransactionsResponse.setInfo(t);
				// juXinLiTransactionsEBTransactionsInfoDao.insert(t);
			}
		} catch (Exception ex) {

			logger.error(
					"info insert setTransactionsEbs to db method, Exception ex:{}",
					ex);
		}
	}

	/**
	 * ���̵�ַ��Ϣ
	 * 
	 * @param juXinLiTransactionsEBResponse
	 * 
	 * @param transactions_bill_no
	 */
	private void setTransactionsAddress(
			JuXinLiTransactionsEBResponse juXinLiTransactionsEBResponse,
			String transactions_bill_no) {
		try {
			List<JuXinLiTransactionsEBAddressResponse> address = juXinLiTransactionsEBResponse
					.getAddress();
			for (JuXinLiTransactionsEBAddressResponse juXinLiTransactionsEBAddressResponse : address) {
				JuXinLiTransactionsEBAddressInfo t = juXinLiTransactionsEBAddressResponse
						.convertToEBAddress(juXinLiTransactionsEBAddressResponse);
				t.setBillNo(transactions_bill_no);
				juXinLiTransactionsEBAddressResponse.setInfo(t);
				// juXinLiTransactionsEBAddressInfoDao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setTransactionsAddress to db method, Exception ex:{}",
					ex);
		}
	}

	private List<JuXinLiBehaviorCheckResponse4_2> setBehaviorCheck4_2(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiBehaviorCheckResponse4_2> data = null;
		try {
			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("behavior_check")),
					JuXinLiBehaviorCheckResponse4_2.class);

			for (JuXinLiBehaviorCheckResponse4_2 item : data) {
				JuXinLiBehaviorCheck info = new JuXinLiBehaviorCheck();
				org.springframework.beans.BeanUtils.copyProperties(item, info);
				info.setBillNo(resp.getTranserialsId());
				item.setInfo(info);
				// juXinLi_4_2_BehaviorCheckInfoDao.insert(info);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setBehaviorCheck4_2 to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	private List<JuXinLiApplicationCheckResponse4_2> setApplicationCheck4_2(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiApplicationCheckResponse4_2> data = null;
		try {
			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("application_check")),
					JuXinLiApplicationCheckResponse4_2.class);

			for (JuXinLiApplicationCheckResponse4_2 item : data) {
				Map<String, Object> checkPoints = item.getCheck_points();

				item.setKey_value((String) checkPoints.get("key_value"));
				JuXinLiApplicationCheck info = new JuXinLiApplicationCheck();
				info.setBill_no(resp.getTranserialsId());
				info.setApp_point(item.getApp_point());
				info.setCheck_points(JSONObject.toJSONString(item
						.getCheck_points()));
				info.setKey_value(item.getKey_value());
				item.setInfo(info);
				// juXinLi_4_2_ApplicationCheckInfoDao.insert(info);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setApplicationCheck4_2 to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	private JuXinLiUserInfoCheckResponse setUserInfoCheck4_2(
			ResponseEntity resp, JSONObject report_data_json) {

		JuXinLiUserInfoCheckResponse data = new JuXinLiUserInfoCheckResponse();

		try {
			JSONObject objUserInfoCheck = (JSONObject) report_data_json
					.get("user_info_check");

			JSONObject checkSearchInfoResponse = (JSONObject) objUserInfoCheck
					.get("check_search_info");
			JSONObject checkBlackInfoResponse = (JSONObject) objUserInfoCheck
					.get("check_black_info");

			data.setCheckSearchInfo(JSONObject.parseObject(
					checkSearchInfoResponse.toJSONString(),
					JuXinLiCheckSearchInfoResponse.class));
			data.setCheckBlackInfo(JSONObject.parseObject(
					checkBlackInfoResponse.toJSONString(),
					JuXinLiCheckBlackInfoResponse.class));

			JuXinLiCheckSearchInfo checkSearchInfo = new JuXinLiCheckSearchInfo();
			org.springframework.beans.BeanUtils.copyProperties(data.getCheckSearchInfo(), checkSearchInfo);
			checkSearchInfo.setBillNo(resp.getTranserialsId());
			data.getCheckSearchInfo().setInfo(checkSearchInfo);
			// juXinLiCheckSearchInfoDao.insert(checkSearchInfo);

			JuXinLiCheckBlackInfo checkBlackInfo = new JuXinLiCheckBlackInfo();
			org.springframework.beans.BeanUtils.copyProperties(data.getCheckBlackInfo(), checkBlackInfo);
			checkBlackInfo.setBillNo(resp.getTranserialsId());
			data.getCheckBlackInfo().setInfo(checkBlackInfo);
			// juXinLiCheckBlackInfoDao.insert(checkBlackInfo);

		} catch (Exception ex) {
			logger.error(
					"info insert setUserInfoCheck4_2 to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	/**
	 * ���з�����trip_info��
	 * 
	 * @param resp
	 * @param report_data_json
	 * @return
	 */
	private List<JuXinLiTripInfoResponse> setTripInfo(ResponseEntity resp,
			JSONObject report_data_json) {

		List<JuXinLiTripInfoResponse> data = null;
		try {
			data = JSONArray.toList(
					JSONArray.fromObject(report_data_json.get("trip_info")),
					JuXinLiTripInfoResponse.class);

			for (JuXinLiTripInfoResponse item : data) {
				JuXinLiTripInfo info = new JuXinLiTripInfo();
				org.springframework.beans.BeanUtils.copyProperties(item, info);
				info.setBill_no(resp.getTranserialsId());
				item.setInfo(info);
				// juXinLiTripInfoDao.insert(info);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setTripInfo to db method, Exception ex:{}", ex);
		}

		return data;
	}

	private List<JuXinLiEbusinessExpenseResponse4_2> setEbusinessExpense4_2(
			ResponseEntity resp, JSONObject report_data_json) {
		// ������Ϊ�������
		List<JuXinLiEbusinessExpenseResponse4_2> data = null;
		try {

			data = JSONArray.toList(JSONArray.fromObject(report_data_json
					.get("ebusiness_expense")),
					JuXinLiEbusinessExpenseResponse4_2.class);

			for (JuXinLiEbusinessExpenseResponse4_2 item : data) {
				JuXinLiEbusinessExpenseInfo4_2 t = new JuXinLiEbusinessExpenseInfo4_2();
				// BeanUtils.copyProperties(item, t);

				t.setAll_category(JSONObject.toJSONString(item
						.getAll_category()));
				t.setAll_amount(item.getAll_amount());
				t.setAll_count(item.getAll_count());
				t.setTrans_mth(item.getTrans_mth());
				t.setBill_no(resp.getTranserialsId());
				item.setInfo(t);
				// juXinLiEbusinessExpenseInfo_4_2Dao.insert(t);
			}
		} catch (Exception ex) {
			logger.error(
					"info insert setEbusinessExpense to db method, Exception ex:{}",
					ex);
		}

		return data;
	}

	private void saveReport4_2(JuXinLiDataResponse resposne,
			ResponseEntity resp, JSONObject report_data_json) {

		// �û���Ϊ��⣨behavior_check��
		resposne.setBehaviorCheckResponse4_2(setBehaviorCheck4_2(resp,
				report_data_json));

		// �û�������⣨application_check��
		resposne.setApplicationCheckResponse4_2(setApplicationCheck4_2(resp,
				report_data_json));

		// �û���Ϣ��⣨user_info_check��
		resposne.setUserInfoResponse(setUserInfoCheck4_2(resp, report_data_json));

		// ���з�����trip_info��
		resposne.setTripInfoResponse(setTripInfo(resp, report_data_json));

		// ����������
		resposne.setEbusinessExpense4_2(setEbusinessExpense4_2(resp,
				report_data_json));

		// ��ϵ���������
		resposne.setContactRegionResponse(setContactRegion(resp,
				report_data_json));

		// ��Ӫ����ϵ���б�
		resposne.setContactListResponse(setContactList(resp, report_data_json));

		// �ջ��˵�ַ
		resposne.setDeliverAddressResponse(setDeliverAddress(resp,
				report_data_json));

		// ͨ����Ϊ���ݷ���
		resposne.setCellBehaviorResponse(setCellBehavior(resp, report_data_json));

		// ��ϵ����Ϣ
		resposne.setCollectionContacResponse(setCollectionContact(resp,
				report_data_json));

	}

	private void saveReport4_0(JuXinLiDataResponse resposne,
			ResponseEntity resp, JSONObject report_data_json) {
		// ��������Դ����
		resposne.setDataSourceResponse(setDataSource(resp, report_data_json));
		// ������Ϊ�������
		resposne.setBehaviorCheckResponse(setBehaviorCheck(resp,
				report_data_json));
		// ��Ϣ�˶�
		resposne.setApplicationCheckResponse(setApplicationCheck(resp,
				report_data_json));
		// ��Ӫ����ϵ���б�
		resposne.setContactListResponse(setContactList(resp, report_data_json));

		// ��������
		resposne.setRecentNeedResponse(setRecentNeed(resp, report_data_json));

		// ͨ����Ϊ���ݷ���
		resposne.setCellBehaviorResponse(setCellBehavior(resp, report_data_json));

		// ��ϵ���������
		resposne.setContactRegionResponse(setContactRegion(resp,
				report_data_json));

		// ��ϵ����Ϣ
		resposne.setCollectionContacResponse(setCollectionContact(resp,
				report_data_json));

		// �ջ��˵�ַ
		resposne.setDeliverAddressResponse(setDeliverAddress(resp,
				report_data_json));

		// ����������
		resposne.setEbusinessExpense(setEbusinessExpense(resp, report_data_json));
	}

}
