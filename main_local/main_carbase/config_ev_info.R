###***将爬虫库新能源电动机配置同步到云********##
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
library(mailR)
library(Rcpp)
library(pinyin)
local_file<-'//User-20170720ma/yck_wash_model_new'
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
local_defin<<-fun_mysql_config_up()$local_defin
local_defin_yun<<-fun_mysql_config_up()$local_defin_yun
local_defin_test<-data.frame(user = 'root',host='192.168.0.111',password= '000000',dbname='test',stringsAsFactors = F)
output_che300<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM config_che300_ev_info;"))
output_che300$ee_battery_quality_assurance<-output_che300$ee_battery_warranty_time
output_autohome<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM config_autohome_ev_info;"))%>%dplyr::select(-url)
plat_match<-fun_mysqlload_query(local_defin_yun,paste0("SELECT id_che300,id_autohome FROM config_plat_id_match where is_only_autohome=1;"))
###处理函数
output_save<-output_che300
dealfun_ev_info<-function(output_save){
  ##电机类型
  output_save$ee_type<-gsub("/| ","",output_save$ee_type)
  output_save$ee_type<-gsub("后","/后",output_save$ee_type)
  ##电池类型
  output_save$ee_fast_charging_time<-gsub("标配|选配|待查","-",output_save$ee_fast_charging_time)
  output_save$ee_battery_type<-str_extract(output_save$ee_battery_type,"三元镍钴锰酸锂|磷酸铁锂|三元锂|锂离子|钴酸锂|锰酸锂|镍钴锰|高压锂|镍氢")
  output_save$ee_battery_type[which(!is.na(output_save$ee_battery_type))]<-paste0(output_save$ee_battery_type[which(!is.na(output_save$ee_battery_type))],'电池')
  output_save$ee_battery_type[which(is.na(output_save$ee_battery_type))]<-"-"
  ##续航
  range<-stringr::str_subset(names(output_save),"range$")
  for(i in 1:length(range)){
    output_save[,which(colnames(output_save)==range[i])]<-gsub("标配|选配","-",output_save[,which(colnames(output_save)==range[i])])
    output_save[,which(colnames(output_save)==range[i])]<-gsub("km|公里","",output_save[,which(colnames(output_save)==range[i])])
    }
  ##质保
  battery_assurance<-data.frame(a=c('三','四','五','六','八','十','待查'),b=c('3','4','5','6','8','10','-'))
  for(i in 1:7){
    output_save$ee_battery_quality_assurance<-gsub(battery_assurance[i,1],battery_assurance[i,2],output_save$ee_battery_quality_assurance)
  }
  output_save$ee_battery_quality_assurance<-gsub("或","",output_save$ee_battery_quality_assurance)
  output_save$ee_battery_quality_assurance<-gsub(" ","",output_save$ee_battery_quality_assurance)
  ##充电
  output_save$ee_fast_charging_time<-gsub("标配|选配","-",output_save$ee_fast_charging_time)
  output_save$ee_slow_charging_time<-gsub("标配|选配","-",output_save$ee_slow_charging_time)
  output_save$ee_fast_charge[which(output_save$ee_fast_charge!='-')]<-paste0(output_save$ee_fast_charge[which(output_save$ee_fast_charge!='-')],"%")
  return(output_save)
}
##补充函数
fun_ev_replenish<-function(data_from,data_to,id_from,id_to){
  data_from<-data_from[,which(colnames(data_from) %in% vars)]
  data_to<-data_to%>%dplyr::left_join(plat_match,by=c('model_id'=id_to))
  data_to<-merge(data_to,data_from,by.x =id_from,by.y='model_id',all=T)%>%filter(!is.na(model_id))
  for (i  in 2:12) {
    data_to[,which(colnames(data_to)==paste0(vars[i],'.x'))][grep('-',data_to[,which(colnames(data_to)==paste0(vars[i],'.x'))])]<-data_to[,which(colnames(data_to)==paste0(vars[i],'.y'))][grep('-',data_to[,which(colnames(data_to)==paste0(vars[i],'.x'))])]
    data_to[,which(colnames(data_to)==paste0(vars[i],'.x'))][which(is.na(data_to[,which(colnames(data_to)==paste0(vars[i],'.x'))]))]<-"-"
  }
  select_field<-c(id_from,stringr::str_subset(names(data_to),"y$"))
  data_to<-data_to%>%dplyr::select(-select_field)
  colnames(data_to)<-gsub('.x','',names(data_to))
  return(data_to)
}

##车300新能源处理
output_che300<-dealfun_ev_info(output_che300)
##汽车之家新能源处理
output_autohome<-dealfun_ev_info(output_autohome)
##需要补充字段
vars<-c("model_id","ee_type","ee_driving_number","ee_layout","ee_battery_type","ee_pure_electric_range","ee_battery_capacity",
        "ee_power_consumption","ee_battery_quality_assurance","ee_fast_charging_time","ee_slow_charging_time","ee_fast_charge")
##车300补充
config_che300_ev_info<-fun_ev_replenish(output_autohome,output_che300,'id_autohome','id_che300')
##汽车之家补充
config_autohome_ev_info<-fun_ev_replenish(output_che300,output_autohome,'id_che300','id_autohome')
##同步到正式云local_defin_test-->local_defin_yun
fun_mysqlload_all(local_file,local_defin_test,config_che300_ev_info,"config_che300_ev_info")
fun_mysqlload_all(local_file,local_defin_test,config_autohome_ev_info,"config_autohome_ev_info")
