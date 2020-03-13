##-------------每周一次增量更新-----------##
##---本代码处理车300详细配置---#
rm(list = ls(all=T))
gc()
library(reshape2)
library(dplyr)
library(RMySQL)
library(stringr)
library(tcltk)
library(raster)
library(truncnorm)
library(rlist)
library(mailR)
local_file<<-'//User-20170720ma/yck_wash_model_new'
#local_file<-gsub("(\\/main|\\/bat|\\/main_local).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
local_defin_yun<<-fun_mysql_config_up()$local_defin_yun
##详细配置处理函数
fun_config_vdatabase_detail_info<-function(model_detail){
  for(i in 1:ncol(model_detail)){
    model_detail[,i]<-gsub("未知|待查","-",model_detail[,i])
    model_detail[,i][which(is.na(model_detail[,i]))]<-"-"
    model_detail[,i][which(nchar(model_detail[,i])==0)]<-"-"
  }
  ##特殊字段
  model_detail$miit_mileage<-gsub("选配|标配","-",model_detail$miit_mileage)
  model_detail$eng_cylinders<-gsub("选配|标配","-",model_detail$eng_cylinders)
  model_detail$bd_min_ground_clearance<-gsub("选配|标配","-",model_detail$eng_cylinders)
  model_detail$eng_valve_per_cylinder<-gsub("选配|标配","-",model_detail$eng_valve_per_cylinder)
  model_detail$bd_fuel_tank_capacity<-gsub("选配|标配","-",model_detail$bd_fuel_tank_capacity)
  model_detail$bd_fuel_tank_capacity<-gsub("\t","",model_detail$bd_fuel_tank_capacity)
  ##最小离地间隙
  model_detail$bd_min_ground_clearance<-gsub(".0","",model_detail$bd_min_ground_clearance)
  ##燃油标号
  model_detail$eng_fuel_no<-str_extract(model_detail$eng_fuel_no,"[0-9]{1,}")
  model_detail$eng_fuel_no[which(is.na(model_detail$eng_fuel_no))]<-"-"
  
  ##最大速度
  model_detail$ba_max_speed<-gsub("选配|标配","-",model_detail$ba_max_speed)
  model_detail$ba_max_speed<-gsub(".0","",model_detail$ba_max_speed)
  model_detail$ba_max_speed[which(nchar(model_detail$ba_max_speed)==0)]<-"-"
  #悬架类型
  model_detail$ch_rear_suspension_type[grep('后：',model_detail$ch_front_suspension_type)]<-model_detail$ch_front_suspension_type[grep('后：',model_detail$ch_front_suspension_type)]
  model_detail$ch_rear_suspension_type[grep('后：',model_detail$ch_front_suspension_type)]<-str_extract(model_detail$ch_front_suspension_type[grep('后：',model_detail$ch_front_suspension_type)],"后：[^\"]+")
  model_detail$ch_front_suspension_type[grep('后：',model_detail$ch_front_suspension_type)]<-str_extract(model_detail$ch_front_suspension_type[grep('后：',model_detail$ch_front_suspension_type)],"[^\"]+后：")
  dealFun_suspension<-function(input_suspension){
    input_suspension<-gsub("悬挂|悬吊","悬架",input_suspension)
    input_suspension<-gsub("带|附|/","+",input_suspension)
    input_suspension<-gsub("前：|后：|未知|\\（|\\）","",input_suspension)
  }
  model_detail$ch_front_suspension_type<-dealFun_suspension(model_detail$ch_front_suspension_type)
  model_detail$ch_rear_suspension_type<-dealFun_suspension(model_detail$ch_rear_suspension_type)
  ##档位个数
  model_detail$gb_gears<-str_extract(model_detail$gb_gears,"[0-9]{1,}")
  model_detail$gb_gears[which(is.na(model_detail$gb_gears))]<-"-"
  model_detail$gb_abbr[grep('-',model_detail$gb_abbr)]<-model_detail$ba_gearbox[grep('-',model_detail$gb_abbr)]
  model_detail$gb_gears[grep('-',model_detail$gb_gears)]<-str_extract(model_detail$gb_abbr[grep('-',model_detail$gb_gears)],"[0-9]{1,}")
  model_detail$gb_gears[which(is.na(model_detail$gb_gears))]<-"-"
  ##变速箱类型
  model_detail$gb_type[grep('-',model_detail$gb_type)]<-str_extract(model_detail$gb_abbr[grep('-',model_detail$gb_type)],
                                                                      "CVT无级变速|无级变速|CVT|自动|手自动一体|手自一体|手动|CVT|CVT|双离合|DSG双离合|G-DCT|DCT|DSG")
  model_detail$gb_type<-gsub("[0-9]挡 ","",model_detail$gb_type)
  model_detail$gb_type[which(is.na(model_detail$gb_type))]<-"-"
  ##整车质保
  model_detail$ba_QA<-gsub("/","或",model_detail$ba_QA)
  model_detail$ba_QA<-gsub("\t|质保|整车","",model_detail$ba_QA)
  model_detail$ba_QA<-gsub("终身","不限年限或不限里程",model_detail$ba_QA)
  ##座位数
  model_detail$bd_seats<-gsub("、","/",model_detail$bd_seats)
  model_detail$bd_seats<-gsub("●","",model_detail$bd_seats)
  model_detail$bd_seats<-gsub("/○","选装",model_detail$bd_seats)
  model_detail$bd_seats[grep('-',model_detail$bd_seats)]<-str_extract(model_detail$ba_structure[grep('-',model_detail$bd_seats)],"[0-9]{1,}座")
  model_detail$bd_seats<-gsub("座","",model_detail$bd_seats)
  model_detail$bd_seats[which(is.na(model_detail$bd_seats))]<-"-"
  ##车门数
  model_detail$bd_doors<-str_extract(model_detail$bd_doors,"[0-9]{1,}")
  model_detail$bd_doors[which(is.na(model_detail$bd_doors))]<-"-"
  model_detail$bd_doors<-gsub("93","-",model_detail$bd_doors)
  model_detail$bd_doors[grep('-',model_detail$bd_doors)]<-str_extract(model_detail$ba_structure[grep('-',model_detail$bd_doors)],"[0-9]{1,}门")
  model_detail$bd_doors<-gsub("门","",model_detail$bd_doors)
  model_detail$bd_doors<-gsub("0","-",model_detail$bd_doors)
  model_detail$bd_doors[which(is.na(model_detail$bd_doors))]<-"-"
  model_detail$ba_lwh<-gsub("\t","",model_detail$ba_lwh)
  model_detail$update_time<-as.character(Sys.Date())
  #return(model_detail=model_detail)
  fun_mysqlload_add(local_file,local_defin_yun,model_detail,"config_vdatabase_yck_detail_info")
  return(1)
}
fun_mailsend_check<-function(input_subject,input_body){
  send.mail(from = "2579104076@qq.com",
            to = c("rozsa@youcku.com"),
            subject = input_subject,
            encoding = 'utf-8',
            body = input_body,
            html = TRUE,
            smtp = list(host.name = "smtp.qq.com",port = 465,
                        user.name ="2579104076@qq.com",passwd = "ugfcamtnpzuhecec",
                        ssl = TRUE,tls =TRUE),
            authenticate = TRUE,
            send = TRUE)
}
##详细配置增量更新处理
if(weekdays(Sys.Date())=='星期四'){
  model_detail_add<-fun_mysqlload_query(local_defin_yun,"SELECT a.* FROM config_che300_detail_info a 
                                  LEFT JOIN config_vdatabase_yck_detail_info b ON a.model_id=b.model_id
                                  WHERE b.model_id IS NULL;")
  vdatabase_model<-fun_mysqlload_query(local_defin_yun,"SELECT * FROM config_vdatabase_yck_model;")
  model_detail_add<-inner_join(vdatabase_model,model_detail_add,by='model_id') %>% dplyr::select(-ba_level,-eng_emission)
  if(nrow(model_detail_add)>0){
    if(tryCatch({fun_config_vdatabase_detail_info(model_detail_add)},error=function(e){0},finally={0})!=1){
      fun_mailsend_check("detail_che300执行错误",'详细配置更新处理异常')
     }
  }
}


