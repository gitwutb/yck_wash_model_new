##投诉部分：12365
fun_else_sync_12365<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT * FROM spider_complain_12365auto WHERE add_time>'",as.character(Sys.Date()-30),"';")),-1) %>% dplyr::select(-url)
  dbDisconnect(loc_channel)
  for (i in 1:dim(output_save)[2]) {
    output_save[,i][which(is.na(output_save[,i]))]<-"\\N"
    output_save[,i]<-gsub("\\,","，",output_save[,i])
    output_save[,i]<-gsub("\\\n","",output_save[,i])
  }
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_12365auto")
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==1){
  if(tryCatch({fun_else_sync_12365()},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：12365')}
}

##经销商报价：汽车之家（本地20/21号抓完，24号开始同步）
fun_else_sync_autoDiscount<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT * FROM discount_rate_history WHERE stat_time='",paste0(str_sub(as.character(Sys.Date()),1,8),'15'),"';")),-1)
  dbDisconnect(loc_channel)
  for (i in 1:dim(output_save)[2]) {
    output_save[,i][which(is.na(output_save[,i]))]<-"\\N"
    output_save[,i]<-gsub("\\,","，",output_save[,i])
    output_save[,i]<-gsub("\\\n","",output_save[,i])
  }
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"discount_rate_history")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'经销商报价-discount_rate_history表更新完毕')}
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==24){
  if(tryCatch({fun_else_sync_autoDiscount(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'经销商报价同步失败：汽车之家')}
}

##汽车销量：搜狐（本地25号抓完，26号开始同步）
fun_else_sync_souhuSalenumber<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT * FROM spider_salesnum_souhu WHERE add_time>'",paste0(str_sub(as.character(Sys.Date()),1,8),'01'),"';")),-1)
  dbDisconnect(loc_channel)
  for (i in 1:dim(output_save)[2]) {
    output_save[,i][which(is.na(output_save[,i]))]<-"\\N"
    output_save[,i]<-gsub("\\,","，",output_save[,i])
    output_save[,i]<-gsub("\\\n","",output_save[,i])
  }
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_salesnum_souhu")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'销量数据-spider_salesnum_souhu表更新完毕')}
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==26){
  if(tryCatch({fun_else_sync_souhuSalenumber(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'销量数据同步失败：搜狐')}
}

##汽车口碑：汽车之家（每月一号同步）
fun_else_sync_autoKoub<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT id,model_id,isbattery,IF(drivenKilometers_appends>=drivekilometer,drivenKilometers_appends,drivekilometer) miles,
                                        boughtdate,SUBSTR(boughtcity_id FROM 1 FOR 2) province_id,SUBSTR(boughtcity_id FROM 1 FOR 4) city_id,visitcount,helpfulcount,commentcount,boughtPrice,
                                        score_spaceScene space,score_powerScene power,score_maneuverabilityScene control,score_oilScene oilconsumption,score_batteryScene eleconsumption ,
                                        score_comfortablenessScene comfortableness,score_apperanceScene apperance,score_internalScene interior,score_costefficientScene costefficient,satisfaction,
                                        append_time comment_time,DATE_FORMAT(add_time,'%Y-%m-%d') add_time from spider_koubei_autohome WHERE DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-5,"%Y-%m-01")),"';")),-1)
  config_model_info<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id,model_name,brand_id,brand_name,series_id,series_name,model_year,model_price from config_autohome_major_info_tmp;"),-1)
  config_district_code<-dbFetch(dbSendQuery(loc_channel,"select SUBSTR(city_code,1,2) province_id,SUBSTR(city_code,1,4) city_id,key_province province,min(key_municipal) city 
             from config_district  group by SUBSTR(city_code,1,4), key_province;"),-1)
  #问题：有些省存在直辖县，如湖北、海南、新疆            
  dbDisconnect(loc_channel)
  config_model_info$model_id<-as.numeric(config_model_info$model_id)
  config_district_codeP<-config_district_code %>% dplyr::select(province_id,province) %>% unique()
  config_district_codeC<-config_district_code %>% dplyr::select(city_id,city) %>% unique()
  output_save<-output_save %>% dplyr::inner_join(config_model_info,by='model_id') %>%
    dplyr::inner_join(config_district_codeP,by='province_id') %>% dplyr::left_join(config_district_codeC,by='city_id')
  output_save$boughtdate<-paste0(as.character(output_save$boughtdate),'-01')
  output_save<- output_save %>% 
    dplyr::select(id,model_id,brand_id,brand_name,series_id,series_name,model_year,model_name,model_price,isbattery,
                  boughtdate,boughtPrice,province,city,miles,visitcount,helpfulcount,commentcount,space,power,control,
                  oilconsumption,eleconsumption,comfortableness,apperance,interior,costefficient,satisfaction,comment_time,add_time)
  fun_mysqlload_add_upd(local_file,local_defin_yun,output_save,"spider_koubei_autohome")
}
if(as.integer(format(Sys.Date(),"%d"))==1){
  if(tryCatch({fun_else_sync_autoKoub(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'口碑数据同步失败：汽车之家')}
}

