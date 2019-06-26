#####本程序主要处理车300车型库中的亮点配置字段
rm(list = ls(all=T))
gc()
library(reshape2)
library(dplyr)
library(RMySQL)
library(stringr)
library(lubridate)
library(parallel)
library(rlist)
local_file<-gsub("(\\/main|\\/bat|\\/main_local).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
local_defin<-fun_mysql_config_up()$local_defin
#function
fun_config_bright_sim<-function(model_id){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  config_bright<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT ieq_reverse_video_system,lt_head_light_cleaning,oeq_power_sunroof,ieq_multi_function_steering_wheel,st_front_passenger_seat_electric_adjust,
        ieq_steering_wheel_shift_paddle,gls_rain_sensing_wiper,ac_rear_air_outlet,gls_rearview_mirror_electric_folding,gls_rearview_mirror_heating,
                                   ieq_rear_parking_radar,lt_led_head_light,oeq_panoramic_sunroof,ieq_front_parking_radar,lt_daytime_running_light,lt_hid,sf_tire_pressure_monitor,
                                   sf_keyless_in,sf_keyless_go,st_waist_support_adjust,lt_head_light_auto_shut,ac_type,st_seats_material,st_front_seats_warm
                                   from config_che300_detail_info WHERE model_id=",model_id,";")),-1)
  config_bright_sim<-dbFetch(dbSendQuery(loc_channel,"select * from config_vdatabase_che300_bright;"),-1)
  dbDisconnect(loc_channel)
  if(nrow(config_bright)==0){
    return_config<-data.frame(model_id=model_id,
                              hl_configs='',
                              hl_configc='')
  }else{
    config_bright<-data.frame(col_names=colnames(config_bright),col_values=as.character(config_bright[1,]))
    config_bright<-merge(config_bright,config_bright_sim)%>%dplyr::select(-col_names)
    config_bright<-config_bright[grep("标配|真皮|自动",config_bright$col_values),]
    return_config<-data.frame(model_id=model_id,
                              hl_configs=paste0(config_bright$col_names_simp,collapse = ";"),
                              hl_configc=paste0(config_bright$col_names_c,collapse = ";")) 
  }
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("UPDATE config_che300_major_info SET hl_configs='",return_config$hl_configs,"',hl_configc='",return_config$hl_configc,"' WHERE model_id=",model_id,";"))
  dbDisconnect(loc_channel)
}

##执行层
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
che300_model_id<-dbFetch(dbSendQuery(loc_channel,"select model_id from config_che300_major_info WHERE hl_configs is NULL;"),-1)
dbDisconnect(loc_channel)
for (id in che300_model_id$model_id) {
  fun_config_bright_sim(id)
  print(id)
}
