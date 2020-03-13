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
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
local_defin<-fun_mysql_config_up()$local_defin
#function
fun_config_bright_sim<-function(model_id){
  config_bright<-fun_mysqlload_query(local_defin,paste0("SELECT ieq_reverse_video_system,lt_head_light_cleaning,oeq_power_sunroof,ieq_multi_function_steering_wheel,st_front_passenger_seat_electric_adjust,
        ieq_steering_wheel_shift_paddle,gls_rain_sensing_wiper,ac_rear_air_outlet,gls_rearview_mirror_electric_folding,gls_rearview_mirror_heating,
                                   ieq_rear_parking_radar,lt_led_head_light,oeq_panoramic_sunroof,ieq_front_parking_radar,lt_daytime_running_light,lt_hid,sf_tire_pressure_monitor,
                                   sf_keyless_in,sf_keyless_go,st_waist_support_adjust,lt_head_light_auto_shut,ac_type,st_seats_material,st_front_seats_warm
                                   from config_che300_detail_info WHERE model_id=",model_id,";"))
  config_bright_sim<-fun_mysqlload_query(local_defin,"select * from config_vdatabase_che300_bright;")
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
  fun_mysqlload_query(local_defin,paste0("UPDATE config_che300_major_info SET hl_configs='",return_config$hl_configs,"',hl_configc='",return_config$hl_configc,"' WHERE model_id=",model_id,";"))
}

##执行层
che300_model_id<-fun_mysqlload_query(local_defin,"select model_id from config_che300_major_info WHERE hl_configs is NULL;")
for (id in che300_model_id$model_id) {
  fun_config_bright_sim(id)
  print(id)
}
