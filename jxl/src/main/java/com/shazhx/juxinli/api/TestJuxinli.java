package com.shazhx.juxinli.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
/**
 * Created by shazhenghui on 2017/5/25.
 */
public class TestJuxinli {

    public TestJuxinli() {
    }


    public static void  main(String[] args){
        //String reqData = "{\"_id\":ObjectId(\"5652e9560cf21dbe4e68b0b3\"),\"user_id\":null,\"borrow_nid\":null,\"channel\":\"all\",\"AccessReportData\":\"{\"note\":\"\",\"report_data\":{\"contact_list\":[],\"data_source\":[{\"status\":\"invalid\",\"account\":\"jd_45e215f9f8ce2\",\"name\":\"京东商城\",\"binding_time\":\"2015-10-1914:10:32\",\"category_value\":\"电子商务\",\"reliability\":\"实名认证\",\"key\":\"jingdong\",\"category_name\":\"e_business\"}],\"behavior_check\":[{\"category\":\"呼叫验证\",\"check_point\":\"朋友圈是否在本地\",\"result\":\"无数据\",\"evidence\":\"未提供家庭住址\"},{\"category\":\"地址验证\",\"check_point\":\"本地居住是否超过1年\",\"result\":\"无数据\",\"evidence\":\"未提供家庭住址\"},{\"category\":\"地址验证\",\"check_point\":\"家庭住所地址是否可以精确定位到\",\"result\":\"无数据\",\"evidence\":\"未提供家庭住址\"},{\"category\":\"地址验证\",\"check_point\":\"工作地点地址是否可以精确定位到\",\"result\":\"无数据\",\"evidence\":\"未提供工作地点地址\"},{\"category\":\"呼叫行为\",\"check_point\":\"长时间关机\",\"result\":\"无数据\",\"evidence\":\"未提供移动运营商数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"绑定号码是新号码\",\"result\":\"无数据\",\"evidence\":\"未提供移动运营商数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"亲属联系人不常联系\",\"result\":\"无数据\",\"evidence\":\"未提供运营商电话数据\"},{\"category\":\"紧急联系人验证\",\"check_point\":\"是否给亲属联系人送过货\",\"result\":\"无数据\",\"evidence\":\"申请表单中未提供亲属联系人的姓名\"},{\"category\":\"呼叫行为\",\"check_point\":\"非亲属联系人不常联系\",\"result\":\"无数据\",\"evidence\":\"未提供运营商电话数据\"},{\"category\":\"紧急联系人验证\",\"check_point\":\"是否给非亲属联系人送过货\",\"result\":\"无数据\",\"evidence\":\"申请表单中未提供非亲属联系人的姓名\"},{\"category\":\"呼叫行为\",\"check_point\":\"主动呼叫过的联系人出现在网贷黑名单上\",\"result\":\"无数据\",\"evidence\":\"无运营商呼叫数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"未呼叫借款用途相关号码\",\"result\":\"无数据\",\"evidence\":\"无运营商呼叫数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"信用卡申报有异常情况\",\"result\":\"无数据\",\"evidence\":\"无运营商呼叫数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"跨省出行申报有异常情况\",\"result\":\"无数据\",\"evidence\":\"无运营商数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"呼叫过澳门电话\",\"result\":\"无数据\",\"evidence\":\"无运营商呼叫数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"呼叫过110\",\"result\":\"无数据\",\"evidence\":\"未绑定移动运营商\"},{\"category\":\"呼叫行为\",\"check_point\":\"呼叫过120\",\"result\":\"无数据\",\"evidence\":\"未绑定移动运营商\"},{\"category\":\"呼叫行为\",\"check_point\":\"呼叫律师相关号码\",\"result\":\"无数据\",\"evidence\":\"无运营商呼叫数据\"},{\"category\":\"呼叫行为\",\"check_point\":\"呼叫法院相关号码\",\"result\":\"无数据\",\"evidence\":\"无运营商呼叫数据\"},{\"category\":\"购买行为\",\"check_point\":\"有效收货数量不足\",\"result\":\"出现\",\"evidence\":\"有效送货数量不足，只有在2015-10收货1次\"},{\"category\":\"购买行为\",\"check_point\":\"购物刷单行为\",\"result\":\"未出现\",\"evidence\":\"未发现疑似刷单记录，每月购物1笔,1个商品\"},{\"category\":\"购买行为\",\"check_point\":\"本人收货次数过少\",\"result\":\"出现\",\"evidence\":\"申请人本人的有效收货数量不足，只有在2015-10收货1次\"},{\"category\":\"购买行为\",\"check_point\":\"本人本地收货地址在2个月内未使用过\",\"result\":\"无数据\",\"evidence\":\"未提供家庭住址\"}],\"collection_contact\":[],\"deliver_address\":[{\"begin_date\":\"2015-10-1914:10:32\",\"total_amount\":599.0,\"end_date\":\"2015-10-2512:57:57\",\"total_count\":1,\"receiver\":[{\"count\":1,\"amount\":599.0,\"name\":\"牟长林\",\"phone_num_list\":[\"13255259857\"]}],\"address\":\"江苏南通市启东市寅阳镇合和镇兴和路222赢山渔家\",\"lat\":31.745112732261,\"lng\":121.81780239713,\"predict_addr_type\":\"工作\"}],\"unionpay_expense\":{},\"_id\":\"56457d4d31adb74ff6ac51b0\",\"person\":{\"province\":\"重庆市\",\"city\":\"市辖区\",\"gender\":\"男\",\"age\":24,\"sign\":\"白羊座\",\"state\":\"重庆市\",\"result\":true,\"real_name\":\"牟长林\",\"region\":\"巴南区\",\"id_card_num\":\"500113199103291119\"},\"main_service\":[],\"ebusiness_expense\":[{\"owner_amount\":599.0,\"all_amount\":599.0,\"family_amount\":0.0,\"family_count\":0,\"all_count\":1,\"others_amount\":0.0,\"owner_count\":1,\"trans_mth\":\"2015-10\",\"others_count\":0}],\"application_check\":[{\"category\":\"号码绑定\",\"check_point\":\"是否收集登记号码的信息\",\"result\":\"无数据\",\"evidence\":\"用户未提供手机号码\"},{\"category\":\"身份验证\",\"check_point\":\"身份证号码是否有效\",\"result\":\"是\",\"evidence\":\"通过国标身份证验证算法\"},{\"category\":\"身份验证\",\"check_point\":\"绑定手机号码是否本人实名认证\",\"result\":\"无数据\",\"evidence\":\"用户未提供手机号码\"},{\"category\":\"身份验证\",\"check_point\":\"运营商登记人身份证号码和姓名是否匹配\",\"result\":\"无数据\",\"evidence\":\"无证据\"},{\"category\":\"身份验证\",\"check_point\":\"学信网登记人身份证号码和姓名是否匹配\",\"result\":\"无数据\",\"evidence\":\"无证据\"},{\"category\":\"身份验证\",\"check_point\":\"申请人是否为普通全日制在校生\",\"result\":\"无数据\",\"evidence\":\"无证据\"},{\"category\":\"呼叫验证\",\"check_point\":\"是否呼叫过住所电话\",\"result\":\"无数据\",\"evidence\":\"未提供家庭电话\"},{\"category\":\"呼叫验证\",\"check_point\":\"是否呼叫过工作电话\",\"result\":\"无数据\",\"evidence\":\"未提供工作电话\"},{\"category\":\"紧急联系人验证\",\"check_point\":\"是否呼叫过亲属联系人\",\"result\":\"无数据\",\"evidence\":\"未提供运营商电话数据\"},{\"category\":\"紧急联系人验证\",\"check_point\":\"是否呼叫过非亲属联系人\",\"result\":\"无数据\",\"evidence\":\"未提供运营商电话数据\"},{\"category\":\"呼叫验证\",\"check_point\":\"是否呼叫过配偶/对象电话\",\"result\":\"无数据\",\"evidence\":\"未提供配偶/对象电话\"},{\"category\":\"网络黑名单\",\"check_point\":\"申请人号码未出现在网贷黑名单上\",\"result\":\"是\",\"evidence\":\"绑定的手机号码[13255259857]未出现在网贷黑名单\"},{\"category\":\"网络黑名单\",\"check_point\":\"申请人身份证未出现在网贷黑名单上\",\"result\":\"是\",\"evidence\":\"提供的身份证号码[500113199103291119]未出现在网贷黑名单\"},{\"category\":\"网络黑名单\",\"check_point\":\"申请人身份证号码未出现在法院黑名单上\",\"result\":\"是\",\"evidence\":\"提供的身份证号码[500113199103291119]未出现在法院黑名单\"},{\"category\":\"地址验证\",\"check_point\":\"家庭住所地址是否出现在有效的收货地址中\",\"result\":\"无数据\",\"evidence\":\"未提供家庭住址\"},{\"category\":\"地址验证\",\"check_point\":\"工作地址是否出现在有效的收货地址中\",\"result\":\"无数据\",\"evidence\":\"未提供工作地址\"}],\"trip_info\":[],\"report\":{\"token\":\"74b33e08eff640aa853af8276015abcd\",\"updt\":\"2015-11-1314:03:56\",\"id\":\"201511131403566799\",\"version\":\"4.0\",\"uid\":null},\"ebusiness_contact\":[],\"recent_need\":[],\"cell_behavior\":[],\"personal_cell_collect\":[],\"contact_region\":[]},\"success\":\"true\"}\",\"addtime\":\"1448274262776\",\"validate\":\"2015-11-2318:24:22\",\"req\":{\"phone\":\"13255259857\",\"idCard\":\"500113199103291119\",\"name\":\"牟长林\",\"channel\":\"all\"}}";
        //String reqData = "{\"_id\":\"564ee36a0cf24d1d244cb4c2\",\"user_id\":null,\"borrow_nid\":null,\"channel\":\"e_business\",\"data\":{\"note\":\"\",\"success\":\"true\",\"raw_data\":{\"members\":{\"status\":\"success\",\"update_time\":\"2015-11-2017:10:01\",\"request_args\":[{\"token\":\"74b33e08eff640aa853af8276015abcd\"},{\"env\":\"www\"}],\"transactions\":[{\"transactions\":[{\"trans_time\":\"2015-10-2512:57:57\",\"update_time\":\"2015-11-1314:03:56\",\"total_price\":100.0,\"receiver_addr\":\"江苏南通市启东市寅阳镇合和镇兴和路222赢山渔家\",\"order_id\":\"10664080928\",\"receiver_name\":\"牟长林\",\"bill_type\":\"\",\"zipcode\":null,\"receiver_title\":null,\"delivery_fee\":8.0,\"receiver_cell_phone\":\"132****9857\",\"payment_type\":\"在线支付\",\"items\":[],\"is_success\":false,\"delivery_type\":\"普通快递\",\"receiver_phone\":null,\"bill_title\":\"\"},{\"trans_time\":\"2015-10-2512:55:08\",\"update_time\":\"2015-11-1314:03:56\",\"total_price\":54.0,\"receiver_addr\":\"江苏南通市启东市寅阳镇合和镇兴和路222赢山渔家\",\"order_id\":\"10694788819\",\"receiver_name\":\"牟长林\",\"bill_type\":\"\",\"zipcode\":null,\"receiver_title\":null,\"delivery_fee\":8.0,\"receiver_cell_phone\":\"132****9857\",\"payment_type\":\"货到付款\",\"items\":[],\"is_success\":false,\"delivery_type\":\"普通快递\",\"receiver_phone\":null,\"bill_title\":\"\"},{\"trans_time\":\"2015-10-1914:21:39\",\"update_time\":\"2015-11-1314:03:56\",\"total_price\":599.0,\"receiver_addr\":\"江苏南通市启东市寅阳镇合和镇兴和路222赢山渔家\",\"order_id\":\"10415362367\",\"receiver_name\":\"牟长林\",\"bill_type\":\"明细\",\"zipcode\":null,\"receiver_title\":null,\"delivery_fee\":0.0,\"receiver_cell_phone\":\"132****9857\",\"payment_type\":\"货到付款\",\"items\":[{\"trans_time\":\"2015-10-1914:21:39\",\"product_price\":599.0,\"product_cnt\":1,\"product_name\":\"小米红米2白色电信4G手机双卡双待\"}],\"is_success\":true,\"delivery_type\":\"普通快递\",\"receiver_phone\":\"\",\"bill_title\":\"个人\"},{\"trans_time\":\"2015-10-1914:10:32\",\"update_time\":\"2015-11-1314:03:56\",\"total_price\":599.0,\"receiver_addr\":\"江苏南通市启东市寅阳镇合和镇兴和路222赢山渔家\",\"order_id\":\"10415547437\",\"receiver_name\":\"牟长林\",\"bill_type\":\"个人\",\"zipcode\":null,\"receiver_title\":null,\"delivery_fee\":0.0,\"receiver_cell_phone\":\"132****9857\",\"payment_type\":\"货到付款\",\"items\":[],\"is_success\":false,\"delivery_type\":\"普通快递\",\"receiver_phone\":null,\"bill_title\":\"明细\"}],\"address\":[{\"province\":null,\"update_time\":\"2015-11-1314:03:56\",\"receiver_addr\":\"江苏南通市启东市寅阳镇合和镇兴和路222赢山渔家\",\"city\":\"江苏南通市启东市寅阳镇\",\"receiver_cell_phone\":\"132****9857\",\"zipcode\":null,\"receiver_title\":null,\"payment_type\":null,\"is_default_address\":false,\"receiver_name\":\"牟长林\",\"receiver_phone\":\"132****9857\"}],\"token\":\"74b33e08eff640aa853af8276015abcd\",\"version\":\"1\",\"datasource\":\"jingdong\",\"basic\":{\"email\":\"\",\"update_time\":\"2015-11-1314:03:56\",\"cell_phone\":null,\"security_level\":null,\"level\":\"铜牌会员\",\"is_validate_real_name\":null,\"website_id\":\"jd_45e215f9f8ce2\",\"nickname\":\"jd_132552pya\",\"register_date\":null,\"real_name\":\"\"},\"juid\":\"\"}],\"error_code\":31200,\"error_msg\":\"请求用户数据成功\"}}},\"addtime\":\"1448010602245\",\"validate\":\"2015-11-2017:10:02\",\"req\":{\"phone\":\"13255259857\",\"idCard\":\"500113199103291119\",\"name\":\"牟长林\",\"channel\":\"e_business\"}}";

        try {
            String encoding = "UTF-8";
            String filePath = "E:\\iwork\\ql\\project\\msgmgt-201612\\data\\juxinli.100.txt";
            File file=new File(filePath);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                Integer count = 0;
                JuxinliClean juxinliClean = new JuxinliClean();
                while((lineTxt = bufferedReader.readLine()) != null){
                    count += 1;
                    List<String> result = juxinliClean.parseDetails(lineTxt, "");
                    for(String rt : result){
                        System.out.println(count + "\t" + rt);
                    }

                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }



/*        Map<Integer, Long> offset = new HashMap<>();
        offset.put(1,1L);
        offset.put(2,2L);
        offset.put(1,11L);
        System.out.println(offset.toString());*/

        //String jsonData = "{\"note\":\"\",\"success\":\"true\",\"raw_data\":{\"members\":{\"status\":\"success\",\"update_time\":\"2016-03-0607:59:58\",\"request_args\":[{\"token\":\"a1d26fef05d2470385e26ee3a5265637\"},{\"env\":\"www\"}],\"transactions\":[],\"error_code\":31204,\"error_msg\":\"没有该用户数据\"}}}";
/*        JuXinLiDataResponse resp = null;
        try {
            resp = new JuxinliService().parse("牟长林", "500113199103291119", "13255259857", "e_business", reqData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<JuXinLiTransactionsEBResponse>  ebtracts = resp.getEb_transactions();
        for(JuXinLiTransactionsEBResponse ebResponse:ebtracts){
            List<JuXinLiTransactionsEBTransactionsResponse> tracts = ebResponse.getTransactions();
            for(JuXinLiTransactionsEBTransactionsResponse trac:tracts){
                String dataJson = JSON.toJSONString(trac.getInfo());
                System.out.println(dataJson);
            }
        }


        System.out.println(resp.toString());*/
    }
}
