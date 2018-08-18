package com.shazhx.juxinli.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.shazhx.juxinli.entity.JuXinLiCallInfo;
import com.shazhx.juxinli.entity.JuXinLiTransactionsEBDatasourceInfo;
import com.shazhx.juxinli.response.*;

public class JuxinliClean implements Serializable {
	public JuxinliClean() {
	}

	public List<String> parseDetails(String info, String validTables) {
		ArrayList<String> details = new ArrayList();
        ArrayList<String> result = new ArrayList();
		JuXinLiDataResponse resp = null;
		new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			JSONObject jsonData = JSON.parseObject(info);

			String e = jsonData.getString("name");
			String idCard = jsonData.getString("idCard");
			String mobile = jsonData.getString("phone");
			String channel = jsonData.getString("channel");
			String addTimeMs = jsonData.getString("addtime");
			String addTime = formatter.format(new Date(Long.valueOf(addTimeMs).longValue()));
			String billNo = jsonData.getJSONObject("_id").getString("$oid"); // {"$oid":"57c6ff640cf20ea32b3f1878"}
			if(e == null || idCard == null || mobile == null || channel == null || addTime == null) {
				return details;
			}

			resp = (new JuxinliService()).parse(e, idCard, mobile, channel, info, billNo);

			JuXinLiQueryResposne juXinLiQueryResposne = resp.getJuXinLiQueryResposne();
			if(juXinLiQueryResposne != null) {
				String juXinLiQueryResposneStr = JSON.toJSONString(juXinLiQueryResposne.getInfo());
				String juXinLiQueryResposneResult = "t_juxinli_query_lib\t" + addTime + "\t" + juXinLiQueryResposneStr;
				details.add(juXinLiQueryResposneResult);
			}
			List Eb_transactions = resp.getEb_transactions();
			String deliverAddressResponse;
			List collectionContacResponse;
			Iterator contactRegionResponse;
			String recentNeedResponse;
			String contactListResponse;
			String tripInfoResponse;
			List contactRegionResponse1;
			Iterator cellBehaviorResponse1;
			if(Eb_transactions != null && Eb_transactions.size() > 0) {
				Iterator transactions = Eb_transactions.iterator();

				while(transactions.hasNext()) {
					JuXinLiTransactionsEBResponse ebusinessExpense4_2 = (JuXinLiTransactionsEBResponse)transactions.next();
					String ebusinessExpense = JSON.toJSONString(ebusinessExpense4_2.getInfo());
					deliverAddressResponse = "t_juxinli_transactions_eb_datasource_lib\t" + addTime + "\t" + ebusinessExpense;
					details.add(deliverAddressResponse);
					collectionContacResponse = ebusinessExpense4_2.getTransactions();
					if(collectionContacResponse.size() > 0) {
						contactRegionResponse = collectionContacResponse.iterator();

						while(contactRegionResponse.hasNext()) {
							JuXinLiTransactionsEBTransactionsResponse cellBehaviorResponse = (JuXinLiTransactionsEBTransactionsResponse)contactRegionResponse.next();
							recentNeedResponse = JSON.toJSONString(cellBehaviorResponse.getInfo());
							contactListResponse = "t_juxinli_transactions_eb_transactions_lib\t" + addTime + "\t" + recentNeedResponse;
							details.add(contactListResponse);
						}
					}

					contactRegionResponse1 = ebusinessExpense4_2.getAddress();
					if(contactRegionResponse1 != null && contactRegionResponse1.size() > 0) {
						cellBehaviorResponse1 = contactRegionResponse1.iterator();

						while(cellBehaviorResponse1.hasNext()) {
							JuXinLiTransactionsEBAddressResponse recentNeedResponse1 = (JuXinLiTransactionsEBAddressResponse)cellBehaviorResponse1.next();
							contactListResponse = JSON.toJSONString(recentNeedResponse1.getInfo());
							tripInfoResponse = "t_juxinli_transactions_eb_address_lib\t" + addTime + "\t" + contactListResponse;
							details.add(tripInfoResponse);
						}
					}

					JuXinLiTransactionsEBBasicResponse cellBehaviorResponse2 = ebusinessExpense4_2.getBasic();
					if(cellBehaviorResponse2 != null) {
						recentNeedResponse = JSON.toJSONString(cellBehaviorResponse2.getInfo());
						contactListResponse = "t_juxinli_transactions_eb_basic_lib\t" + addTime + "\t" + recentNeedResponse;
						details.add(contactListResponse);
					}

					JuXinLiTransactionsEBDatasourceInfo recentNeedResponse2 = ebusinessExpense4_2.getInfo();
					if(recentNeedResponse2 != null) {
						contactListResponse = JSON.toJSONString(recentNeedResponse2);
						tripInfoResponse = "t_juxinli_transactions_eb_datasource_lib\t" + addTime + "\t" + contactListResponse;
						details.add(tripInfoResponse);
					}
				}
			}

			List transactions1 = resp.getTransactions();
			String userInfoResponse;
			String juXinLiCheckSearchInfoResponse;
			String juXinLiCheckBlackInfoResponse;
			String collectionContacResponse1;
			List cellBehaviorResponse3;
			Iterator recentNeedResponse4;
			List recentNeedResponse5;
			Iterator contactListResponse2;
			List contactListResponse3;
			Iterator tripInfoResponse2;
			if(transactions1 != null && transactions1.size() > 0) {
				Iterator ebusinessExpense4_21 = transactions1.iterator();

				while(ebusinessExpense4_21.hasNext()) {
					JuXinLiTransactionsResponse ebusinessExpense1 = (JuXinLiTransactionsResponse)ebusinessExpense4_21.next();
					deliverAddressResponse = JSON.toJSONString(ebusinessExpense1.getInfo());
					collectionContacResponse1 = "t_juxinli_transactions_datasource_lib\t" + addTime + "\t" + deliverAddressResponse;
					details.add(collectionContacResponse1);
					contactRegionResponse1 = ebusinessExpense1.getCalls();
					if(contactRegionResponse1 != null && contactRegionResponse1.size() > 0) {
						cellBehaviorResponse1 = contactRegionResponse1.iterator();

						while(cellBehaviorResponse1.hasNext()) {
							JuXinLiTransactionsCallsResponse recentNeedResponse3 = (JuXinLiTransactionsCallsResponse)cellBehaviorResponse1.next();
							contactListResponse = JSON.toJSONString(recentNeedResponse3.getInfo());
							tripInfoResponse = "t_juxinli_transactions_calls_lib\t" + addTime + "\t" + contactListResponse;
							details.add(tripInfoResponse);
						}
					}

					cellBehaviorResponse3 = ebusinessExpense1.getNets();
					if(cellBehaviorResponse3 != null && cellBehaviorResponse3.size() > 0) {
						recentNeedResponse4 = cellBehaviorResponse3.iterator();

						while(recentNeedResponse4.hasNext()) {
							JuXinLiTransactionsNetsResponse contactListResponse1 = (JuXinLiTransactionsNetsResponse)recentNeedResponse4.next();
							tripInfoResponse = JSON.toJSONString(contactListResponse1.getInfo());
							userInfoResponse = "t_juxinli_transactions_nets_lib\t" + addTime + "\t" + tripInfoResponse;
							details.add(userInfoResponse);
						}
					}

					recentNeedResponse5 = ebusinessExpense1.getTransactions();
					if(recentNeedResponse5 != null && recentNeedResponse5.size() > 0) {
						contactListResponse2 = recentNeedResponse5.iterator();

						while(contactListResponse2.hasNext()) {
							JuxinliTransactionsJuidResponse tripInfoResponse1 = (JuxinliTransactionsJuidResponse)contactListResponse2.next();
							userInfoResponse = JSON.toJSONString(tripInfoResponse1.getInfo());
							juXinLiCheckSearchInfoResponse = "t_juxinli_transactions_juid\t" + addTime + "\t" + userInfoResponse;
							details.add(juXinLiCheckSearchInfoResponse);
						}
					}

					contactListResponse3 = ebusinessExpense1.getSmses();
					if(contactListResponse3 != null && contactListResponse3.size() > 0) {
						tripInfoResponse2 = contactListResponse3.iterator();

						while(tripInfoResponse2.hasNext()) {
							JuXinLiTransactionsSmsesResponse userInfoResponse1 = (JuXinLiTransactionsSmsesResponse)tripInfoResponse2.next();
							juXinLiCheckSearchInfoResponse = JSON.toJSONString(userInfoResponse1.getInfo());
							juXinLiCheckBlackInfoResponse = "t_juxinli_transactions_smses_lib\t" + addTime + "\t" + juXinLiCheckSearchInfoResponse;
							details.add(juXinLiCheckBlackInfoResponse);
						}
					}

					JuXinLiTransactionsBasicResponse tripInfoResponse3 = ebusinessExpense1.getBasic();
					if(tripInfoResponse3 != null) {
						userInfoResponse = JSON.toJSONString(tripInfoResponse3.getInfo());
						juXinLiCheckSearchInfoResponse = "t_juxinli_transactions_basic_lib\t" + addTime + "\t" + userInfoResponse;
						details.add(juXinLiCheckSearchInfoResponse);
					}
				}
			}

			List ebusinessExpense4_22 = resp.getEbusinessExpense4_2();
			String contactRegionResponse2;
			if(ebusinessExpense4_22 != null && ebusinessExpense4_22.size() > 0) {
				Iterator ebusinessExpense2 = ebusinessExpense4_22.iterator();
				while(ebusinessExpense2.hasNext()) {
					JuXinLiEbusinessExpenseResponse4_2 deliverAddressResponse1 = (JuXinLiEbusinessExpenseResponse4_2)ebusinessExpense2.next();
					collectionContacResponse1 = JSON.toJSONString(deliverAddressResponse1.getInfo());
					contactRegionResponse2 = "t_juxinli_ebusiness_expense_lib_4_2\t" + addTime + "\t" + collectionContacResponse1;
					details.add(contactRegionResponse2);
				}
			}

			List ebusinessExpense3 = resp.getEbusinessExpense();
			String cellBehaviorResponse4;
			if(ebusinessExpense3 != null && ebusinessExpense3.size() > 0) {
				Iterator deliverAddressResponse2 = ebusinessExpense3.iterator();

				while(deliverAddressResponse2.hasNext()) {
					JuXinLiEbusinessExpenseResponse collectionContacResponse2 = (JuXinLiEbusinessExpenseResponse)deliverAddressResponse2.next();
					contactRegionResponse2 = JSON.toJSONString(collectionContacResponse2.getInfo());
					cellBehaviorResponse4 = "t_juxinli_ebusiness_expense_lib\t" + addTime + "\t" + contactRegionResponse2;
					details.add(cellBehaviorResponse4);
				}
			}

			List deliverAddressResponse3 = resp.getDeliverAddressResponse();
			if(deliverAddressResponse3 != null && deliverAddressResponse3.size() > 0) {
				Iterator collectionContacResponse3 = deliverAddressResponse3.iterator();

				label331:
				while(true) {
					do {
						do {
							if(!collectionContacResponse3.hasNext()) {
								break label331;
							}

							JuXinLiDeliverAddressResponse contactRegionResponse3 = (JuXinLiDeliverAddressResponse)collectionContacResponse3.next();
							cellBehaviorResponse4 = JSON.toJSONString(contactRegionResponse3.getInfo());
							recentNeedResponse = "t_juxinli_deliver_address_lib\t" + addTime + "\t" + cellBehaviorResponse4;
							details.add(recentNeedResponse);
							contactListResponse3 = contactRegionResponse3.getReceiver();
						} while(contactListResponse3 == null);
					} while(contactListResponse3.size() <= 0);

					tripInfoResponse2 = contactListResponse3.iterator();

					while(tripInfoResponse2.hasNext()) {
						JuXinLiReceiverResponse userInfoResponse2 = (JuXinLiReceiverResponse)tripInfoResponse2.next();
						juXinLiCheckSearchInfoResponse = JSON.toJSONString(userInfoResponse2.getInfo());
						juXinLiCheckBlackInfoResponse = "t_juxinli_receiver_lib\t" + addTime + "\t" + juXinLiCheckSearchInfoResponse;
						details.add(juXinLiCheckBlackInfoResponse);
					}
				}
			}

			collectionContacResponse = resp.getCollectionContacResponse();
			String applicationCheckResponse4_2;
			List tripInfoResponse4;
			Iterator userInfoResponse3;
			if(collectionContacResponse != null && collectionContacResponse.size() > 0) {
				contactRegionResponse = collectionContacResponse.iterator();

				label308:
				while(true) {
					do {
						do {
							if(!contactRegionResponse.hasNext()) {
								break label308;
							}

							JuXinLiCollectionContacResponse cellBehaviorResponse5 = (JuXinLiCollectionContacResponse)contactRegionResponse.next();
							recentNeedResponse = JSON.toJSONString(cellBehaviorResponse5.getInfo());
							contactListResponse = "t_juxinli_collection_contact_lib\t" + addTime + "\t" + recentNeedResponse;
							details.add(contactListResponse);
							tripInfoResponse4 = cellBehaviorResponse5.getContact_details();

							List<JuXinLiCallResponse> contactDetails = cellBehaviorResponse5.getContact_details();
							if(contactDetails != null && contactDetails.size() >0){
								for(JuXinLiCallResponse cr: contactDetails){
									String callInfo = JSON.toJSONString(cr.getInfo());
									String callInfoStr =  "t_juxinli_call_lib\t" + addTime + "\t" + callInfo;
									details.add(callInfoStr);
								}
							}
						} while(tripInfoResponse4 == null);
					} while(tripInfoResponse4.size() <= 0);

					userInfoResponse3 = tripInfoResponse4.iterator();

					while(userInfoResponse3.hasNext()) {
						JuXinLiCallResponse juXinLiCheckSearchInfoResponse1 = (JuXinLiCallResponse)userInfoResponse3.next();
						juXinLiCheckBlackInfoResponse = JSON.toJSONString(juXinLiCheckSearchInfoResponse1.getInfo());
						applicationCheckResponse4_2 = "t_juxinli_behavior_lib\t" + addTime + "\t" + juXinLiCheckBlackInfoResponse;
						details.add(applicationCheckResponse4_2);
					}
				}
			}

			contactRegionResponse1 = resp.getContactRegionResponse();
			if(contactRegionResponse1 != null && contactRegionResponse1.size() > 0) {
				cellBehaviorResponse1 = contactRegionResponse1.iterator();

				while(cellBehaviorResponse1.hasNext()) {
					JuXinLiContactRegionResponse recentNeedResponse6 = (JuXinLiContactRegionResponse)cellBehaviorResponse1.next();
					contactListResponse = JSON.toJSONString(recentNeedResponse6.getInfo());
					tripInfoResponse = "t_juxinli_contact_region_lib\t" + addTime + "\t" + contactListResponse;
					details.add(tripInfoResponse);
				}
			}

			cellBehaviorResponse3 = resp.getCellBehaviorResponse();
			String applicationCheckResponse;
			String behaviorCheckResponse4_2;
			if(cellBehaviorResponse3 != null && cellBehaviorResponse3.size() > 0) {
				recentNeedResponse4 = cellBehaviorResponse3.iterator();

				label275:
				while(true) {
					List juXinLiCheckSearchInfoResponse2;
					do {
						do {
							if(!recentNeedResponse4.hasNext()) {
								break label275;
							}

							JuXinLiCellBehaviorResponse contactListResponse4 = (JuXinLiCellBehaviorResponse)recentNeedResponse4.next();
							tripInfoResponse = JSON.toJSONString(contactListResponse4.getInfo());
							userInfoResponse = "t_juxinli_cell_behavior_lib\t" + addTime + "\t" + tripInfoResponse;
							details.add(userInfoResponse);
							juXinLiCheckSearchInfoResponse2 = contactListResponse4.getBehavior();
						} while(juXinLiCheckSearchInfoResponse2 == null);
					} while(juXinLiCheckSearchInfoResponse2.size() <= 0);

					Iterator juXinLiCheckBlackInfoResponse1 = juXinLiCheckSearchInfoResponse2.iterator();

					while(juXinLiCheckBlackInfoResponse1.hasNext()) {
						JuXinLiBehaviorResponse applicationCheckResponse4_21 = (JuXinLiBehaviorResponse)juXinLiCheckBlackInfoResponse1.next();
						applicationCheckResponse = JSON.toJSONString(applicationCheckResponse4_21.getInfo());
						behaviorCheckResponse4_2 = "t_juxinli_behavior_lib\t" + addTime + "\t" + applicationCheckResponse;
						details.add(behaviorCheckResponse4_2);
					}
				}
			}

			recentNeedResponse5 = resp.getRecentNeedResponse();
			String behaviorCheckResponse;
			if(recentNeedResponse5 != null && recentNeedResponse5.size() > 0) {
				contactListResponse2 = recentNeedResponse5.iterator();

				label252:
				while(true) {
					List juXinLiCheckBlackInfoResponse2;
					do {
						do {
							if(!contactListResponse2.hasNext()) {
								break label252;
							}

							JuXinLiRecentNeedResponse tripInfoResponse5 = (JuXinLiRecentNeedResponse)contactListResponse2.next();
							userInfoResponse = JSON.toJSONString(tripInfoResponse5.getInfo());
							juXinLiCheckSearchInfoResponse = "t_juxinli_recent_need_lib\t" + addTime + "\t" + userInfoResponse;
							details.add(juXinLiCheckSearchInfoResponse);
							juXinLiCheckBlackInfoResponse2 = tripInfoResponse5.getDemands_info();
						} while(juXinLiCheckBlackInfoResponse2 == null);
					} while(juXinLiCheckBlackInfoResponse2.size() <= 0);

					Iterator applicationCheckResponse4_22 = juXinLiCheckBlackInfoResponse2.iterator();

					while(applicationCheckResponse4_22.hasNext()) {
						JuXinLiDemandsInfoResponse applicationCheckResponse1 = (JuXinLiDemandsInfoResponse)applicationCheckResponse4_22.next();
						behaviorCheckResponse4_2 = JSON.toJSONString(applicationCheckResponse1.getInfo());
						behaviorCheckResponse = "t_juxinli_demands_info_lib\t" + addTime + "\t" + behaviorCheckResponse4_2;
						details.add(behaviorCheckResponse);
					}
				}
			}

			contactListResponse3 = resp.getContactListResponse();
			if(contactListResponse3 != null && contactListResponse3.size() > 0) {
				tripInfoResponse2 = contactListResponse3.iterator();

				while(tripInfoResponse2.hasNext()) {
					JuXinLiContactListResponse userInfoResponse4 = (JuXinLiContactListResponse)tripInfoResponse2.next();
					juXinLiCheckSearchInfoResponse = JSON.toJSONString(userInfoResponse4.getInfo());
					juXinLiCheckBlackInfoResponse = "t_juxinli_contact_list_lib\t" + addTime + "\t" + juXinLiCheckSearchInfoResponse;
					details.add(juXinLiCheckBlackInfoResponse);
				}
			}

			tripInfoResponse4 = resp.getTripInfoResponse();
			if(tripInfoResponse4 != null && tripInfoResponse4.size() > 0) {
				userInfoResponse3 = tripInfoResponse4.iterator();

				while(userInfoResponse3.hasNext()) {
					JuXinLiTripInfoResponse juXinLiCheckSearchInfoResponse3 = (JuXinLiTripInfoResponse)userInfoResponse3.next();
					juXinLiCheckBlackInfoResponse = JSON.toJSONString(juXinLiCheckSearchInfoResponse3.getInfo());
					applicationCheckResponse4_2 = "t_juxinli_trip_info_lib\t" + addTime + "\t" + juXinLiCheckBlackInfoResponse;
					details.add(applicationCheckResponse4_2);
				}
			}

			JuXinLiUserInfoCheckResponse userInfoResponse5 = resp.getUserInfoResponse();
			JuXinLiCheckSearchInfoResponse juXinLiCheckSearchInfoResponse4 = userInfoResponse5.getCheckSearchInfo();
			if(juXinLiCheckSearchInfoResponse4 != null) {
				juXinLiCheckBlackInfoResponse = JSON.toJSONString(juXinLiCheckSearchInfoResponse4.getInfo());
				applicationCheckResponse4_2 = "t_juxinli_check_search_info_lib\t" + addTime + "\t" + juXinLiCheckBlackInfoResponse;
				details.add(applicationCheckResponse4_2);
			}

			JuXinLiCheckBlackInfoResponse juXinLiCheckBlackInfoResponse3 = userInfoResponse5.getCheckBlackInfo();
			if(juXinLiCheckBlackInfoResponse3 != null) {
				applicationCheckResponse4_2 = JSON.toJSONString(juXinLiCheckBlackInfoResponse3.getInfo());
				applicationCheckResponse = "t_juxinli_check_black_info_lib\t" + addTime + "\t" + applicationCheckResponse4_2;
				details.add(applicationCheckResponse);
			}

			List applicationCheckResponse4_23 = resp.getApplicationCheckResponse4_2();
			String dataSourceResponse;
			if(applicationCheckResponse4_23 != null && applicationCheckResponse4_23.size() > 0) {
				Iterator applicationCheckResponse2 = applicationCheckResponse4_23.iterator();

				while(applicationCheckResponse2.hasNext()) {
					JuXinLiApplicationCheckResponse4_2 behaviorCheckResponse4_21 = (JuXinLiApplicationCheckResponse4_2)applicationCheckResponse2.next();
					behaviorCheckResponse = JSON.toJSONString(behaviorCheckResponse4_21.getInfo());
					dataSourceResponse = "t_juxinli_application_check_lib\t" + addTime + "\t" + behaviorCheckResponse;
					details.add(dataSourceResponse);
				}
			}

			List applicationCheckResponse3 = resp.getApplicationCheckResponse();
			String item;
			if(applicationCheckResponse3 != null && applicationCheckResponse3.size() > 0) {
				Iterator behaviorCheckResponse4_22 = applicationCheckResponse3.iterator();

				while(behaviorCheckResponse4_22.hasNext()) {
					JuXinLiBehaviorCheckResponse behaviorCheckResponse1 = (JuXinLiBehaviorCheckResponse)behaviorCheckResponse4_22.next();
					dataSourceResponse = JSON.toJSONString(behaviorCheckResponse1.getInfo());
					item = "t_juxinli_behavior_application_check_lib\t" + addTime + "\t" + dataSourceResponse;
					details.add(item);
				}
			}

			List behaviorCheckResponse4_23 = resp.getBehaviorCheckResponse4_2();
			String item1;
			if(behaviorCheckResponse4_23 != null && behaviorCheckResponse4_23.size() > 0) {
				Iterator behaviorCheckResponse2 = behaviorCheckResponse4_23.iterator();

				while(behaviorCheckResponse2.hasNext()) {
					JuXinLiBehaviorCheckResponse4_2 dataSourceResponse1 = (JuXinLiBehaviorCheckResponse4_2)behaviorCheckResponse2.next();
					item = JSON.toJSONString(dataSourceResponse1.getInfo());
					item1 = "t_juxinli_behavior_check_lib\t" + addTime + "\t" + item;
					details.add(item1);
				}
			}

			List behaviorCheckResponse3 = resp.getBehaviorCheckResponse();
			String dataJson;
			if(behaviorCheckResponse3 != null && behaviorCheckResponse3.size() > 0) {
				Iterator dataSourceResponse2 = behaviorCheckResponse3.iterator();

				while(dataSourceResponse2.hasNext()) {
					JuXinLiBehaviorCheckResponse item2 = (JuXinLiBehaviorCheckResponse)dataSourceResponse2.next();
					item1 = JSON.toJSONString(item2.getInfo());
					dataJson = "t_juxinli_behavior_check_lib\t" + addTime + "\t" + item1;
					details.add(dataJson);
				}
			}

			List dataSourceResponse3 = resp.getDataSourceResponse();
			if(dataSourceResponse3 != null && dataSourceResponse3.size() > 0) {
				Iterator item3 = dataSourceResponse3.iterator();

				while(item3.hasNext()) {
					JuXinLiDataSourceResponse item4 = (JuXinLiDataSourceResponse)item3.next();
					dataJson = JSON.toJSONString(item4.getInfo());
					String toString = "t_juxinli_data_source_lib\t" + addTime + "\t" + dataJson;
					details.add(toString);
				}
			}
		} catch (Exception var36) {
			var36.printStackTrace();
		}

		if(validTables.equals("")){
		    return details;
        }else {
            String[] tables = validTables.split(",");
            List<String> validList = new ArrayList<>();
            for (String table : tables) {
                validList.add(table);
            }
            for (String dt : details) {
                String table = dt.split("\t")[0];
                if (validList.contains(table)) {
                    result.add(dt);
                }
            }
            return result;
        }
	}
}
