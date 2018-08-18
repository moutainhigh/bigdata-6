
-- report_reportpersonalinfo
DROP TABLE tmp.report_reportpersonalinfo_tmp;
create table tmp.report_reportpersonalinfo_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,reporttime string,reportSN string,querytime string,IDnumber string,IDtype string,marital string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_reportpersonalinfo;
create table dw_db.dw_report_reportpersonalinfo(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,reporttime string,reportSN string,querytime string,IDnumber string,IDtype string,marital string)
COMMENT 'report_reportpersonalinfo parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_creditrecord_summary
DROP TABLE tmp.report_creditrecord_summary_tmp;
create table tmp.report_creditrecord_summary_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,overdueTotal string,activeTotal string,accountTotal string,overdue90Total string,guarantee string,assetDisposal string,compensate string,type string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_creditrecord_summary;
create table dw_db.dw_report_creditrecord_summary(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,overdueTotal string,activeTotal string,accountTotal string,overdue90Total string,guarantee string,assetDisposal string,compensate string,type string)
COMMENT 'report_creditrecord_summary parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_creditrecord_detail
DROP TABLE tmp.report_creditrecord_detail_tmp;
create table tmp.report_creditrecord_detail_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,assetDisposal string,compensate string,guarantee string,noOverdueDetails string,overdueDetails string,type string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_creditrecord_detail;
create table dw_db.dw_report_creditrecord_detail(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,assetDisposal string,compensate string,guarantee string,noOverdueDetails string,overdueDetails string,type string)
COMMENT 'report_creditrecord_detail parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_publicrecord_summary
DROP TABLE tmp.report_publicrecord_summary_tmp;
create table tmp.report_publicrecord_summary_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,tax string,judgment string,enforcement string,punishment string,telecom string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_publicrecord_summary;
create table dw_db.dw_report_publicrecord_summary(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,tax string,judgment string,enforcement string,punishment string,telecom string)
COMMENT 'report_publicrecord_summary parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_publicrecord_detail
DROP TABLE tmp.report_publicrecord_detail_tmp;
create table tmp.report_publicrecord_detail_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,tax string,judgment string,enforcement string,punishment string,telecom string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_publicrecord_detail;
create table dw_db.dw_report_publicrecord_detail(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,tax string,judgment string,enforcement string,punishment string,telecom string)
COMMENT 'report_publicrecord_detail parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_queryecord_summary
DROP TABLE tmp.report_queryecord_summary_tmp;
create table tmp.report_queryecord_summary_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,individual string,organization string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_queryecord_summary;
create table dw_db.dw_report_queryecord_summary(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,individual string,organization string)
COMMENT 'report_queryecord_summary parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_queryecord_detail
DROP TABLE tmp.report_queryecord_detail_tmp;
create table tmp.report_queryecord_detail_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,date string,operator string,reason string,type string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_queryecord_detail;
create table dw_db.dw_report_queryecord_detail(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,intro string,date string,operator string,reason string,type string)
COMMENT 'report_queryecord_detail parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_extend
DROP TABLE tmp.report_extend_tmp;
create table tmp.report_extend_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,cusName string,cusIdNo string,binderName string,binderIdNo string,operatorId string,extend_1 string,extend_2 string,extend_3 string,querytime string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_extend;
create table dw_db.dw_report_extend(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,cusName string,cusIdNo string,binderName string,binderIdNo string,operatorId string,extend_1 string,extend_2 string,extend_3 string,querytime string)
COMMENT 'report_extend parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_risk
DROP TABLE tmp.report_risk_tmp;
create table tmp.report_risk_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,reporttime string,reportSN string,querytime string,IDnumber string,IDtype string,marital string,overdues_creditOrgCounts string,overdues_creditOrgCounts200 string,overdues_creditAmts string,overdues_creditAmts200 string,overdues_creditCountsM60 string,overdues_creditCountsM60D90 string,overdues_loanCounts string,overdues_loanAmts string,overdues_loanCountsM60 string,overdues_loanCountsM60D90 string,overdues_countsM60 string,overdues_countsM60D90 string,debts_creditLimitMax string,debts_creditLimitTotal string,debts_creditOrgCounts string,debts_creditLimitUsed string,debts_creditLimitUseRate string,debts_loanAmts string,debts_loanAmtsNoSettle string,debts_loanCounts string,debts_loanBalances string,debts_loanBalanceCounts string,debts_loanBalancesMortgage string,debts_loanBalancesCar string,debts_loanBalancesBiz string,debts_loanBalancesOther string,debts_loanBalancesMonth string,debts_loanBalancesMortgageMonth string,debts_loanBalancesCarMonth string,debts_loanBalancesBizMonth string,debts_loanBalancesOtherMonth string,creditLoanHis_creditMOB string,creditLoanHis_loanMOB string,creditLoanNeeds_creditOrgCountsM3 string,creditLoanNeeds_creditLimitTotalM3 string,creditLoanNeeds_loanCountsM3 string,creditLoanNeeds_loanAmtsM3 string,creditLoanNeeds_loanQueriesM3 string,creditLoanNeeds_selfQueriesM3 string,others_guarantees string,others_guaranteeAmts string,others_month6TaxAmts string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_risk;
create table dw_db.dw_report_risk(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,reporttime string,reportSN string,querytime string,IDnumber string,IDtype string,marital string,overdues_creditOrgCounts string,overdues_creditOrgCounts200 string,overdues_creditAmts string,overdues_creditAmts200 string,overdues_creditCountsM60 string,overdues_creditCountsM60D90 string,overdues_loanCounts string,overdues_loanAmts string,overdues_loanCountsM60 string,overdues_loanCountsM60D90 string,overdues_countsM60 string,overdues_countsM60D90 string,debts_creditLimitMax string,debts_creditLimitTotal string,debts_creditOrgCounts string,debts_creditLimitUsed string,debts_creditLimitUseRate string,debts_loanAmts string,debts_loanAmtsNoSettle string,debts_loanCounts string,debts_loanBalances string,debts_loanBalanceCounts string,debts_loanBalancesMortgage string,debts_loanBalancesCar string,debts_loanBalancesBiz string,debts_loanBalancesOther string,debts_loanBalancesMonth string,debts_loanBalancesMortgageMonth string,debts_loanBalancesCarMonth string,debts_loanBalancesBizMonth string,debts_loanBalancesOtherMonth string,creditLoanHis_creditMOB string,creditLoanHis_loanMOB string,creditLoanNeeds_creditOrgCountsM3 string,creditLoanNeeds_creditLimitTotalM3 string,creditLoanNeeds_loanCountsM3 string,creditLoanNeeds_loanAmtsM3 string,creditLoanNeeds_loanQueriesM3 string,creditLoanNeeds_selfQueriesM3 string,others_guarantees string,others_guaranteeAmts string,others_month6TaxAmts string)
COMMENT 'report_risk parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;


-- ===========================================================================================================================================================================================
-- report_risk_loanBalanceInfos
DROP TABLE tmp.report_risk_loanBalanceInfos_tmp;
create table tmp.report_risk_loanBalanceInfos_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,org string,amts string,type string,balances string,debtMonths string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_risk_loanBalanceInfos;
create table dw_db.dw_report_risk_loanBalanceInfos(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,querytime string,IDnumber string,org string,amts string,type string,balances string,debtMonths string)
COMMENT 'report_risk_loanBalanceInfos parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_basic
DROP TABLE tmp.report_structure_basic_tmp;
create table tmp.report_structure_basic_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,orgId string,operatorId string,reportSn string,queryTime string,reportTime string,idtype string,idno string,idname string,marital string,assetFlag string,compensateFlag string,creditFlag string,loanFlag string,guaranteeFlag string,taxFlag string,judgmentFlag string,enforcementFlag string,punishmentFlag string,telecomFlag string,checks string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_basic;
create table dw_db.dw_report_structure_basic(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,orgId string,operatorId string,reportSn string,queryTime string,reportTime string,idtype string,idno string,idname string,marital string,assetFlag string,compensateFlag string,creditFlag string,loanFlag string,guaranteeFlag string,taxFlag string,judgmentFlag string,enforcementFlag string,punishmentFlag string,telecomFlag string,checks string,isfail string)
COMMENT 'report_structure_basic parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_general
DROP TABLE tmp.report_structure_general_tmp;
create table tmp.report_structure_general_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,idno string,queryTime string,assetTotal string,compensateTotal string,creditTotal string,creditActive string,creditOverdue string,creditOverdue90 string,creditGuarantee string,mortgageTotal string,mortgageActive string,mortgageOverdue string,mortgageOverdue90 string,mortgageGuarantee string,otherloanTotal string,otherloanActive string,otherloanOverdue string,otherloanOverdue90 string,otherloanGuarantee string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_general;
create table dw_db.dw_report_structure_general(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,idno string,queryTime string,assetTotal string,compensateTotal string,creditTotal string,creditActive string,creditOverdue string,creditOverdue90 string,creditGuarantee string,mortgageTotal string,mortgageActive string,mortgageOverdue string,mortgageOverdue90 string,mortgageGuarantee string,otherloanTotal string,otherloanActive string,otherloanOverdue string,otherloanOverdue90 string,otherloanGuarantee string,isfail string)
COMMENT 'report_structure_general parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_assets
DROP TABLE tmp.report_structure_assets_tmp;
create table tmp.report_structure_assets_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,company string,debtDate string,debtAmount string,lastRepayment string,balance string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_assets;
create table dw_db.dw_report_structure_assets(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,company string,debtDate string,debtAmount string,lastRepayment string,balance string,isfail string)
COMMENT 'report_structure_assets parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_compensates
DROP TABLE tmp.report_structure_compensates_tmp;
create table tmp.report_structure_compensates_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,organization string,lastCompensate string,sumRepayment string,lastRepayment string,balance string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_compensates;
create table dw_db.dw_report_structure_compensates(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,organization string,lastCompensate string,sumRepayment string,lastRepayment string,balance string,isfail string)
COMMENT 'report_structure_compensates parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_guarantees
DROP TABLE tmp.report_structure_guarantees_tmp;
create table tmp.report_structure_guarantees_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,guaranteeTime string,guaranteeIdtype string,guaranteeIdname string,guaranteeIdno string,organization string,contractAmount string,guaranteeAmount string,principaAmount string,recordTime string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_guarantees;
create table dw_db.dw_report_structure_guarantees(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,guaranteeTime string,guaranteeIdtype string,guaranteeIdname string,guaranteeIdno string,organization string,contractAmount string,guaranteeAmount string,principaAmount string,recordTime string,isfail string)
COMMENT 'report_structure_guarantees parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;


-- ===========================================================================================================================================================================================
-- report_structure_credits
DROP TABLE tmp.report_structure_credits_tmp;
create table tmp.report_structure_credits_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,creditStatus string,cardType string,accountStatus string,issueBank string,issueTime string,accountType string,recordOrCancellation string,creditAmount string,creditUsed string,overAmount string,overMonth string,overMonth90days string,declaration string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_credits;
create table dw_db.dw_report_structure_credits(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,creditStatus string,cardType string,accountStatus string,issueBank string,issueTime string,accountType string,recordOrCancellation string,creditAmount string,creditUsed string,overAmount string,overMonth string,overMonth90days string,declaration string,isfail string)
COMMENT 'report_structure_credits parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_loans
DROP TABLE tmp.report_structure_loans_tmp;
create table tmp.report_structure_loans_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,creditStatus string,accountStatus string,loanBank string,loanType string,loanTime string,loanAmount string,loanBalance string,loanDeadline string,recordOrCancellation string,overAmount string,overMonth string,overMonth90days string,declaration string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_loans;
create table dw_db.dw_report_structure_loans(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,creditStatus string,accountStatus string,loanBank string,loanType string,loanTime string,loanAmount string,loanBalance string,loanDeadline string,recordOrCancellation string,overAmount string,overMonth string,overMonth90days string,declaration string,isfail string)
COMMENT 'report_structure_loans parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_taxs
DROP TABLE tmp.report_structure_taxs_tmp;
create table tmp.report_structure_taxs_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,government string,recordTime string,amount string,t_idNo string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_taxs;
create table dw_db.dw_report_structure_taxs(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,government string,recordTime string,amount string,t_idNo string,isfail string)
COMMENT 'report_structure_taxs parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;


-- ===========================================================================================================================================================================================
-- report_structure_judgments
DROP TABLE tmp.report_structure_judgments_tmp;
create table tmp.report_structure_judgments_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,court string,docketNo string,docketCause string,filingWay string,filingTime string,filingResult string,filingEffective string,litigationSubject string,litigationAmount string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_judgments;
create table dw_db.dw_report_structure_judgments(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,court string,docketNo string,docketCause string,filingWay string,filingTime string,filingResult string,filingEffective string,litigationSubject string,litigationAmount string,isfail string)
COMMENT 'report_structure_judgments parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_enforcements
DROP TABLE tmp.report_structure_enforcements_tmp;
create table tmp.report_structure_enforcements_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,court string,docketNo string,docketCause string,filingWay string,filingTime string,docketStatus string,applyEnforceSubject string,executedEnforceSubject string,applyEnforeAmount string,executedEnforceAmount string,closedTime string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_enforcements;
create table dw_db.dw_report_structure_enforcements(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,court string,docketNo string,docketCause string,filingWay string,filingTime string,docketStatus string,applyEnforceSubject string,executedEnforceSubject string,applyEnforeAmount string,executedEnforceAmount string,closedTime string,isfail string)
COMMENT 'report_structure_enforcements parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;


-- ===========================================================================================================================================================================================
-- report_structure_punishments
DROP TABLE tmp.report_structure_punishments_tmp;
create table tmp.report_structure_punishments_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,organization string,docketNo string,reconsiderationFlag string,reconsideration string,punishmentContent string,punishmentAmount string,punishmentEffective string,punishmentDeadline string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_punishments;
create table dw_db.dw_report_structure_punishments(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,organization string,docketNo string,reconsiderationFlag string,reconsideration string,punishmentContent string,punishmentAmount string,punishmentEffective string,punishmentDeadline string,isfail string)
COMMENT 'report_structure_punishments parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;

-- ===========================================================================================================================================================================================
-- report_structure_telecoms
DROP TABLE tmp.report_structure_telecoms_tmp;
create table tmp.report_structure_telecoms_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,telco string,business string,recordTime string,businessTime string,amount string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_telecoms;
create table dw_db.dw_report_structure_telecoms(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,telco string,business string,recordTime string,businessTime string,amount string,isfail string)
COMMENT 'report_structure_telecoms parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;


-- ===========================================================================================================================================================================================
-- report_structure_traces
DROP TABLE tmp.report_structure_traces_tmp;
create table tmp.report_structure_traces_tmp(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,t_queryTime string,queryOperator string,queryReason string,isfail string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

DROP TABLE dw_db.dw_report_structure_traces;
create table dw_db.dw_report_structure_traces(domain string,messageId string,idCard string,name string,sellerId string,timestamp string,success string,queryTime string,idno string,original string,t_queryTime string,queryOperator string,queryReason string,isfail string)
COMMENT 'report_structure_traces parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(idCard)  INTO 32 BUCKETS
STORED AS PARQUET;