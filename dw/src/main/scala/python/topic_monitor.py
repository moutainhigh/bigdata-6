from hdfs import *

client = Client("http://10.15.176.124:50070",root="/",timeout=100,session=False)

topics = ['bigdata_msgmgt_mobanker-customer_userloginfo', 'bigdata_rca_jsystem_message', 'bigdata_sensor_mobanker-customer_fillcontacts', 'bigdata_sensor_mobanker-microsite_borrowclickstats', 'bigdata_sensor_mobanker-microsite_commonrecordevent', 'biz-uzone-wechat-channel-subscribe', 'biz_crawler_collect', 'biz_crawler_query', 'biz_crawler_query_new', 'biz_crawler_query_success', 'biz_crawler_query_success_new', 'biz_microsite_grabcallrds', 'biz_microsite_grabcontacts', 'biz_microsite_grablbs', 'biz_microsite_grabmobile', 'biz_microsite_grabshareuserinfologs', 'biz_microsite_grabsms', 'biz_microsite_zhuoyi_appinfo', 'biz_microsite_zhuoyi_simplesms', 'mobanker_auth_biz_data', 'mobanker_authadptation_biz_data', 'msgmgt-msg-retry_save', 'pbccrc_report', 'rule_engine_preanalysis_result', 'rule_engine_result', 'star_auth_fushu_raw']

