###***将爬虫库车型库同步到111及云********##
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
library(mailR)
library(Rcpp)
library(pinyin)
local_file<<-gsub("(\\/main|\\/bat|\\/main_local).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
#local_file<-'//User-20170720ma/yck_wash_model_new'
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
local_defin<<-fun_mysql_config_up()$local_defin
local_defin_yun<<-fun_mysql_config_up()$local_defin_yun
source(paste0(local_file,"/main_local/main_carbase/fun_vdatabase_sync.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/main_local/main_carbase/fun_else_sync.R"),echo=FALSE,encoding="utf-8")
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
config_distr<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_distr_all<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
dbDisconnect(loc_channel)

#每周四更新（22号之后的周四更新全量）
if(as.integer(format(Sys.Date(),"%d"))>22){
  min_model_year=as.integer(format(Sys.Date(),"%Y"))-10}else{
    min_model_year=as.integer(format(Sys.Date(),"%Y"))-3
  }
if(weekdays(Sys.Date())=='星期四'){
  input_v<-c("fun_vdata_sync_autohome","fun_vdata_sync_czb","fun_vdata_sync_souhu",
             "fun_vdata_sync_yiche","fun_vdata_sync_youxin","fun_vdata_sync_che58",
             "fun_vdata_sync_12365","fun_vdata_sync_autoowner","fun_vdata_sync_firstauto")
  for (i in 1:length(input_v)) {
    return_res<-tryCatch({eval(parse(text = paste0(input_v[i],"(local_defin_yun,min_model_year)")))},error=function(e){0},finally={0})
    if(return_res!=1){fun_mailsend("车型库同步异常（云）",paste0(input_v[i],'车型库更新失败'))}
  }
  #车300及详细配置更新
  if(tryCatch({fun_vdata_sync(local_defin_yun,'config_che300_major_info',min_model_year)},error=function(e){0},finally={0})!=1){
    fun_mailsend("车型库同步异常（云）",'车300车型库更新失败')}
  if(tryCatch({fun_vdata_sync_detail_c300(min_model_year)},error=function(e){0},finally={0})!=1){
    fun_mailsend("车型库同步异常（云）",'车300详细配置车型库更新失败')}
  # if(tryCatch({fun_vdata_sync(local_defin111,'config_autohome_major_info_tmp',min_model_year)},error=function(e){0},finally={0})!=1){
  #   fun_mailsend("车型库同步异常（111）",'汽车之家车型库更新失败')}
  if(tryCatch({fun_vdata_sync_detail_au(min_model_year)},error=function(e){0},finally={0})!=1){
    fun_mailsend("车型库同步异常（云111）",'汽车之家详细配置车型库更新失败')}
}
