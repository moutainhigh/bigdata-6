create table es_rule.biz_rule_engine_result(taskId string,taskTime string,appName string,appRequestId string,inputParam string,outputParam string,ruleVersion string)
COMMENT 'biz_rule_engine_result parquet file'
PARTITIONED BY(productType STRING,yr STRING,mn STRING,dt STRING)
CLUSTERED BY(taskId)  INTO 32 BUCKETS
STORED AS PARQUET;



create table es_rule.biz_rule_engine_result_tmp(taskId string,taskTime string,productType string,appName string,appRequestId string,inputParam string,outputParam string,ruleVersion string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';



-- 规则引擎中的极光引擎模型数据清洗
create table mysql_db.jg_ascore_v1_input_output(user_id string,borrow_nid string,taskTime string,F0001 string,F0002 double,F0003 double,F0004 double,F0005 double,F0006 double,F0007 double,F0008 double,F0009 double,F0010 double,F0011 double,F0012 double,F0013 double,F0014 double,F0015 double,F0016 double,F0017 double,F0018 double,F0019 double,F0020 double,F0021 double,F0022 double,F0023 double,F0024 double,F0025 double,F0026 double,F0027 double,F0028 double,F0029 double,F0030 double,F0031 double,F0032 double,F0033 double,F0034 double,F0035 double,F0036 double,F0037 double,F0038 double,F0039 double,model_total_score double,output01 double,output02 double,output03 double,output04 double,output05 double,output06 double,output07 double,output08 double,output09 double,output10 double,output11 double,output12 double,output13 double,output14 double,output15 double,output16 double,output17 double,output18 double,output19 double,output20 double,output21 double,output22 double,output23 double,output24 double,output25 double,output26 double,output27 double,output28 double,output29 double,output30 double)
COMMENT 'jg_ascore_v1_input_output parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(borrow_nid)  INTO 32 BUCKETS
STORED AS PARQUET;

-- 先处理历史数据
insert OVERWRITE table mysql_db.jg_ascore_v1_input_output PARTITION(yr,mn,dt)
SELECT
split(appRequestId,'_')[0] user_id,
split(appRequestId,'_')[1] borrow_nid,
taskTime as tasktime,
get_json_object(inputparam,'$.F0001') F0001,
get_json_object(inputparam,'$.F0002') F0002,
get_json_object(inputparam,'$.F0003') F0003,
get_json_object(inputparam,'$.F0004') F0004,
get_json_object(inputparam,'$.F0005') F0005,
get_json_object(inputparam,'$.F0006') F0006,
get_json_object(inputparam,'$.F0007') F0007,
get_json_object(inputparam,'$.F0008') F0008,
get_json_object(inputparam,'$.F0009') F0009,
get_json_object(inputparam,'$.F0010') F0010,
get_json_object(inputparam,'$.F0011') F0011,
get_json_object(inputparam,'$.F0012') F0012,
get_json_object(inputparam,'$.F0013') F0013,
get_json_object(inputparam,'$.F0014') F0014,
get_json_object(inputparam,'$.F0015') F0015,
get_json_object(inputparam,'$.F0016') F0016,
get_json_object(inputparam,'$.F0017') F0017,
get_json_object(inputparam,'$.F0018') F0018,
get_json_object(inputparam,'$.F0019') F0019,
get_json_object(inputparam,'$.F0020') F0020,
get_json_object(inputparam,'$.F0021') F0021,
get_json_object(inputparam,'$.F0022') F0022,
get_json_object(inputparam,'$.F0023') F0023,
get_json_object(inputparam,'$.F0024') F0024,
get_json_object(inputparam,'$.F0025') F0025,
get_json_object(inputparam,'$.F0026') F0026,
get_json_object(inputparam,'$.F0027') F0027,
get_json_object(inputparam,'$.F0028') F0028,
get_json_object(inputparam,'$.F0029') F0029,
get_json_object(inputparam,'$.F0030') F0030,
get_json_object(inputparam,'$.F0031') F0031,
get_json_object(inputparam,'$.F0032') F0032,
get_json_object(inputparam,'$.F0033') F0033,
get_json_object(inputparam,'$.F0034') F0034,
get_json_object(inputparam,'$.F0035') F0035,
get_json_object(inputparam,'$.F0036') F0036,
get_json_object(inputparam,'$.F0037') F0037,
get_json_object(inputparam,'$.F0038') F0038,
get_json_object(inputparam,'$.F0039') F0039,
get_json_object(outputparam,'$.model_total_score') model_total_score,
get_json_object(outputparam,'$.model_detail_score[0]') output01,
get_json_object(outputparam,'$.model_detail_score[1]') output02,
get_json_object(outputparam,'$.model_detail_score[2]') output03,
get_json_object(outputparam,'$.model_detail_score[3]') output04,
get_json_object(outputparam,'$.model_detail_score[4]') output05,
get_json_object(outputparam,'$.model_detail_score[5]') output06,
get_json_object(outputparam,'$.model_detail_score[6]') output07,
get_json_object(outputparam,'$.model_detail_score[7]') output08,
get_json_object(outputparam,'$.model_detail_score[8]') output09,
get_json_object(outputparam,'$.model_detail_score[9]') output10,
get_json_object(outputparam,'$.model_detail_score[10]') output11,
get_json_object(outputparam,'$.model_detail_score[11]') output12,
get_json_object(outputparam,'$.model_detail_score[12]') output13,
get_json_object(outputparam,'$.model_detail_score[13]') output14,
get_json_object(outputparam,'$.model_detail_score[14]') output15,
get_json_object(outputparam,'$.model_detail_score[15]') output16,
get_json_object(outputparam,'$.model_detail_score[16]') output17,
get_json_object(outputparam,'$.model_detail_score[17]') output18,
get_json_object(outputparam,'$.model_detail_score[18]') output19,
get_json_object(outputparam,'$.model_detail_score[19]') output20,
get_json_object(outputparam,'$.model_detail_score[20]') output21,
get_json_object(outputparam,'$.model_detail_score[21]') output22,
get_json_object(outputparam,'$.model_detail_score[22]') output23,
get_json_object(outputparam,'$.model_detail_score[23]') output24,
get_json_object(outputparam,'$.model_detail_score[24]') output25,
get_json_object(outputparam,'$.model_detail_score[25]') output26,
get_json_object(outputparam,'$.model_detail_score[26]') output27,
get_json_object(outputparam,'$.model_detail_score[27]') output28,
get_json_object(outputparam,'$.model_detail_score[28]') output29,
get_json_object(outputparam,'$.model_detail_score[29]') output30,
yr as yr,
mn as mn,
dt as dt
FROM
es_rule.biz_rule_engine_result where producttype='JG_Ascore_V1';

