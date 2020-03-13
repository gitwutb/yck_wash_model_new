#********车型库同步函数(每周四同步一次)*************#
fun_vdata_sync<-function(input_ip,input_name,min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM config_che300_major_info WHERE model_year>= ",min_model_year,";")) %>% dplyr::select(-add_time)
  fun_mysqlload_add_upd(local_file,input_ip,output_save,input_name)
  return(1)
}
##车型库同步函数-汽车之家详细配置
fun_autohome_bseries_info<-function(){
  ##****汽车之家**品牌/车系ID增加--####
  brand_add<-fun_mysqlload_query(local_defin_yun,"SELECT a.brandid,a.Initial,a.brand_name FROM
                                 (SELECT DISTINCT Initial,brandid,brand_name FROM config_autohome_major_info_tmp) a
                                 LEFT JOIN config_autohome_yck_brand b ON a.brandid=b.brandid WHERE b.brandid IS NULL;")
  if(nrow(brand_add)>0){
    brand_add<-brand_add %>% dplyr::mutate(car_country='无')
    fun_mysqlload_add(local_file,local_defin_yun,brand_add,"config_autohome_yck_brand")
  }
  ##车系ID增加--####
  series_add<-fun_mysqlload_query(local_defin_yun,"SELECT s.series_id,s.series_group_name,s.series_name,s.brandid,s.brand_name,s.car_level,s.is_green from 
                                  (SELECT DISTINCT series_id,series_group_name,series_name,brandid,brand_name,`level` car_level,is_green FROM config_autohome_major_info_tmp a 
                                  INNER JOIN config_autohome_detail_info b on a.model_id=b.autohome_id where `level`!='-' and `level`!='无' and model_price!=0) s
                                  LEFT JOIN config_autohome_yck_series c ON s.series_id=c.series_id WHERE c.series_id IS NULL;")
  if(nrow(series_add)>0){
    series_add$series_group_name[grep('进口',series_add$series_name)]<-
      paste0('进口',series_add$series_group_name[grep('进口',series_add$series_name)])
    series_add$series_group_name<-gsub('进口进口进口|进口进口','进口',series_add$series_group_name)
    series_add$series_name<-gsub('\\(进口)','',series_add$series_name)
    series_add$series_name<-toupper(series_add$series_name)
    is_import<-str_extract(series_add$series_group_name,'进口')
    is_import[is_import=='进口']<-1
    is_import[which(is.na(is_import))]<-0
    series_add$is_green<-"无"
    config_autohome_yck_series<-series_add %>% dplyr::select(series_id,series_group_name,series_name,car_level,is_green) %>% dplyr::mutate(is_import=is_import)
    fun_mysqlload_add(local_file,local_defin_yun,config_autohome_yck_series,"config_autohome_yck_series")
  }
  return(1)
}
fun_vdata_sync_detail_au<-function(min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT b.* FROM config_autohome_major_info_tmp a
  INNER JOIN config_autohome_detail_info b ON a.model_id=b.autohome_id WHERE a.model_year>=",min_model_year,";"))
  fun_mysqlload_add(local_file,local_defin_yun,output_save,"config_autohome_detail_info")
  output_save1<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM config_autohome_color WHERE color_outside IS NOT NULL"))
  fun_mysqlload_add(local_file,local_defin_yun,output_save1,"config_autohome_color")
  #品牌车系拆分
  tryCatch({fun_autohome_bseries_info()},error=function(e){fun_mailsend("同步到IT系统的车型库异常",paste0('汽车之家-品牌/车系ID增加，请及时处理:',e))},finally={0})
  return(1)
}
##车型库同步函数-车300详细配置
fun_vdata_sync_detail_c300<-function(min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT b.* FROM config_che300_major_info a
  INNER JOIN config_che300_detail_info b ON a.model_id=b.model_id WHERE a.model_year>=",min_model_year,";"))
  fun_mysqlload_add_upd(local_file,local_defin_yun,output_save,"config_che300_detail_info")
  return(1)
}
####其它车型库更新
fun_vdata_sync_autohome<-function(input_ip,min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT a.model_id,a.brand_id brandid,a.brand_name,a.brand_letter Initial,
   a.series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,a.model_price,a.status,b.engine liter,
   b.gear_box auto,b.environmental_standards_org discharge_standard,c.model_id is_green,b.fuel FROM config_autohome_major_info_tmp a
   INNER JOIN config_autohome_detail_info b ON a.model_id=b.autohome_id
   LEFT JOIN config_autohome_ev_info c ON a.model_id=c.model_id WHERE a.model_year>= ",min_model_year,";"))
  output_save$is_green<-ifelse(is.na(output_save$is_green)==T,0,1)
  output_save$fuel<-gsub(".*纯电.*","1",output_save$fuel)
  output_save$fuel<-gsub(".*油电.*","2",output_save$fuel)
  output_save$fuel<-gsub(".*插电.*","3",output_save$fuel)
  output_save$fuel<-gsub(".*增程.*","4",output_save$fuel)
  output_save$is_green<-ifelse(output_save$fuel%in%c(1:4),output_save$fuel,output_save$is_green)
  output_save$is_green<-as.numeric(output_save$is_green)
  output_save <- output_save %>% dplyr::select(-fuel)
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  output_save$auto[which(output_save$is_green==1)]<-'电动'
  ##是否在售
  output_save$status<-ifelse(output_save$status==''|is.na(output_save$status)==T,'停售','在售')
  output_save$status<-gsub('车型','',output_save$status)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(output_save$is_green==1)]<-'-'
  ##排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_autohome_major_info_tmp")
  return(1)
}

fun_vdata_sync_czb<-function(input_ip,min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand brand_name,a.brand_letter Initial,
 a.company series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,a.model_price,'-' liter,
   '-' auto,'-' discharge_standard FROM config_chezhibao_major_info a WHERE a.model_year>= ",min_model_year,";"))
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它","",output_save$model_year)
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_chezhibao_major_info")
  return(1)
}

fun_vdata_sync_souhu<-function(input_ip,min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand_name brand_name,a.initial Initial,
 a.series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,a.model_price,a.displacement liter,
   a.auto,'-' discharge_standard FROM config_souhu_major_info a WHERE a.model_year>= ",min_model_year,";"))
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它","",output_save$model_year)
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_souhu_major_info")
  return(1)
}

fun_vdata_sync_yiche<-function(input_ip,min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand_name brand_name,a.init Initial,
 a.series_group series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,a.model_price,'-' liter,
   a.gearbox auto,a.discharge_standard FROM config_yiche_major_info a WHERE a.model_year>= ",min_model_year,";"))
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它|其他","",output_save$model_year)
  output_save$model_year<-str_extract(output_save$model_year,"[1-2][0-9][0-9][0-9]")
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_yiche_major_info")
  return(1)
}

fun_vdata_sync_youxin<-function(input_ip,min_model_year){
  sourceCpp(paste0(local_file,"/config/config_fun/pinyin.cpp"))
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand_name brand_name,'-' Initial,
 '' series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,'' model_price,'-' liter,
   '-' auto,'-' discharge_standard FROM config_youxin_major_info_tmp a WHERE a.model_year>= ",min_model_year,";"))
  ##首字母
  output_save$Initial<-substr(as.character(sapply(output_save$brand_name,getLetter)),1,1)
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它|其他","",output_save$model_year)
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_youxin_major_info_tmp")
  return(1)
}

fun_vdata_sync_che58<-function(input_ip,min_model_year){
  sourceCpp(paste0(local_file,"/config/config_fun/pinyin.cpp"))
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand_name brand_name,'-' Initial,
 '' series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,a.model_price,'-' liter,
   '-' auto,'-' discharge_standard FROM config_che58_major_info a WHERE a.model_year>= ",min_model_year,";"))
  ##首字母
  output_save$Initial<-substr(as.character(sapply(output_save$brand_name,getLetter)),1,1)
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它|其他","",output_save$model_year)
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_che58_major_info")
  return(1)
}

fun_vdata_sync_autoowner<-function(input_ip,min_model_year){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand_name brand_name,'-' Initial,
   '' series_group_name,a.series_id,a.series_name,0 model_year,a.model_name,a.model_price,'-' liter,
   a.gearbox auto,'-' discharge_standard FROM config_autoowner_major_info_tmp a ;"))
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它|其他","",output_save$model_year)
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_autoowner_major_info_tmp")
  return(1)
}

fun_vdata_sync_12365<-function(input_ip,min_model_year){
  sourceCpp(paste0(local_file,"/config/config_fun/pinyin.cpp"))
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT  a.model_id,a.brand_id brandid,a.brand_name brand_name,'-' Initial,
 '' series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,'' model_price,'-' liter,
   '-' auto,'-' discharge_standard FROM config_auto12365_major_info_tmp a WHERE a.model_year>= ",min_model_year,";"))
  ##首字母
  output_save$Initial<-substr(as.character(sapply(output_save$brand_name,getLetter)),1,1)
  ##获取手自动
  output_save$auto<-paste0(output_save$model_name,output_save$auto)
  output_save$auto<-dealFun_auto(output_save$auto)
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它|其他","",output_save$model_year)
  #排放标准
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_auto12365_major_info_tmp")
  return(1)
}
fun_vdata_sync_firstauto<-function(input_ip,min_model_year){
  sourceCpp(paste0(local_file,"/config/config_fun/pinyin.cpp"))
  output_save<-fun_mysqlload_query(local_defin,"SELECT a.model_id,a.brand_id brandid,a.brand_name brand_name,'-' Initial,
           series_group series_group_name,a.series_id,a.series_name,a.model_year,a.model_name,a.model_price,liter,
            auto,discharge_standard FROM config_firstauto_major_info a;")
  ##首字母
    output_save$Initial<-substr(as.character(sapply(output_save$brand_name,getLetter)),1,1)
  ##手自动
  output_save$auto[which(output_save$auto=='')]<-str_extract(output_save$model_name[which(output_save$auto=='')],"A/MT|MT")
  output_save$auto<-gsub("A/MT","自动",output_save$auto)
  output_save$auto<-gsub("MT","手动",output_save$auto)
  output_save$auto[which(grepl("A/MT",output_save$model_name) & output_save$auto=='手动')]<-"自动"
  ##排量
  output_save$liter<-str_extract(output_save$liter,"[0-9][.][0-9]")
  output_save$liter[which(is.na(output_save$liter))]<-output_save$model_name[which(is.na(output_save$liter))]
  output_save$liter<-str_extract(output_save$model_name,"[0-9][.][0-9]")
  ##年份
  output_save$model_year[which(output_save$model_year=='0')]<-''
  output_save$model_year<-gsub("其它|其他","",output_save$model_year)
  #排放标准
  dealFun_discharge_standard<-function(input_model_name,input_discharge_standard){
    car_OBD<-str_extract(input_model_name,"OBD")
    car_OBD[which(is.na(car_OBD))]<-""
    car_discharge_standard<-str_extract(input_model_name,"(国|欧)(Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(型|)|京(5|五|V)")
    input_model_name<-gsub("(国|欧)(Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(型|)|京(5|五|V)","",input_model_name)
    ##填写的排量标准----------------------------------------------------------------------------------------------------
    car_discharge<-input_discharge_standard
    if(length(grep('国|欧|京',car_discharge))==0){
      car_discharge<-car_discharge_standard
    }else{if(length(grep('国|欧|京',car_discharge))!=length(car_discharge)){car_discharge[-grep('国|欧|京',car_discharge)]<-car_discharge_standard[-grep('国|欧|京',car_discharge)]}}
    if(length(grep('国|欧|京',car_discharge))!=length(car_discharge)){car_discharge[-grep('国|欧|京',car_discharge)]<-NA}
    car_discharge<-gsub("化油器","",car_discharge)
    car_discharge<-gsub("3|III|Ⅲ","三",car_discharge)
    car_discharge<-gsub("2|Ⅱ|II","二",car_discharge)
    car_discharge<-gsub("4|IV|Ⅳ","四",car_discharge)
    car_discharge<-gsub("6|VI|Ⅵ","六",car_discharge)
    car_discharge<-gsub("5|V|Ⅴ","五",car_discharge)
    car_discharge<-gsub("1|I|Ⅰ","一",car_discharge)  
    car_discharge<-gsub("\\+OBD|\\)","",car_discharge)
    car_discharge<-gsub("\\带OBD|\\)","",car_discharge)
    car_discharge<-gsub("\\(","\\/",car_discharge)
    return(list(return_qx_name=input_model_name,car_discharge=car_discharge,car_OBD=car_OBD))
  }
  output_save$discharge_standard<-dealFun_discharge_standard(output_save$model_name,output_save$discharge_standard)$car_discharge
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"config_firstauto_major_info")
  return(1)
}
#fun_mysqlload_all(local_file,local_defin_yun,output_save,"config_firstauto_major_info")

