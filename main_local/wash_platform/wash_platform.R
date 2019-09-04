rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
library(reshape2)
local_file<<-gsub("(\\/bat|\\/main).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/matchFun_vdatabase.R"),echo=TRUE,encoding="utf-8")
local_defin<<-fun_mysql_config_up()$local_defin
local_defin_yun<<-fun_mysql_config_up()$local_defin_yun
data_new<-Sys.Date()%>%as.character()
#全局变量
source(paste0(local_file,"/main_local/wash_platform/washfun_platform.R"),echo=FALSE,encoding="utf-8")
rm_che58<<- read.csv(paste0(local_file,"/config/config_file/reg_che58.csv",sep=""),header = T,sep = ",")
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
#out_rrc<<- read.csv(paste0(local_file,"/config/config_file/out_rrc.csv"),header = T,sep = ",")
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
rm_series_rule<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
config_distr<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_distr_all<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_series_bcountry<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT yck_brandid,car_country FROM config_vdatabase_yck_brand"),-1)
dbDisconnect(loc_channel)

#执行函数，异常邮件抛出#(每三天执行一次)
if(as.integer(format(Sys.Date(),"%d"))%%3==0){
  input_v<-c("washfun_che168","washfun_che58","washfun_csp","washfun_czb",'washfun_guazi','washfun_rrc','washfun_youxin','washfun_yiche')
  for (i in 1:length(input_v)) {
    return_res<-tryCatch({eval(parse(text = paste0(input_v[i],"()")))},error=function(e){0},finally={0})
    if(return_res!=1){fun_mailsend("二手车价格平台清洗异常",paste0(input_v[i],'平台清洗失败'))}
  }
}