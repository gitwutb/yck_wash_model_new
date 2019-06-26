#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
#help(package="dplyr")
#读取数据
library(RMySQL)
library(mailR)
local_file<-gsub("\\/bat|\\/main\\/.*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/matchFun_vdatabase.R"),echo=TRUE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
local_defin_yun<-fun_mysql_config_up()$local_defin_yun
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
out_rrc<<- read.csv(paste0(local_file,"/config/config_file/out_rrc.csv"),header = T,sep = ",")
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
rm_series_rule<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
id_z<<-che300%>%dplyr::select(id_che300=car_id)
dbDisconnect(loc_channel)
fun_plat_vdatabase<-function(input_tablename,input_idname){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_czb<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT model_id car_id,brand_name,series_name,model_name model_name_t,model_price,model_year,
                             liter,auto car_auto,discharge_standard,series_id FROM ",input_tablename," a;")),-1)
  id_benchmark<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT id_che300,",input_idname,",match_type_ah,is_only_ah FROM config_plat_id_match WHERE match_type_ah NOT IN(0,4);")),-1)
  match_seriesid<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT DISTINCT a.series_id,c.brand_name temp_brand,c.series_name temp_series FROM ",input_tablename," a
 INNER JOIN config_plat_id_match b ON a.model_id=b.",input_idname,"
 INNER JOIN config_vdatabase_yck_major_info c ON b.id_che300=c.model_id GROUP BY series_id;")),-1)
  dbDisconnect(loc_channel)
  id<-data.frame(car_id=setdiff(yck_czb$car_id,id_benchmark$input_idname))
  yck_czb<-inner_join(yck_czb,id,by="car_id")
  id<-data.frame(car_id=setdiff(che300$car_id,id_benchmark$id_che300))
  che300<-inner_join(che300,id,by="car_id")
  yck_czb<-left_join(yck_czb,match_seriesid,by='series_id')
  yck_czb$brand_name[!is.na(yck_czb$temp_brand)]<-yck_czb$temp_brand[!is.na(yck_czb$temp_brand)]
  yck_czb$series_name[!is.na(yck_czb$temp_brand)]<-yck_czb$temp_series[!is.na(yck_czb$temp_brand)]
  yck_czb<-yck_czb %>% dplyr::select(-series_id,-temp_brand,-temp_series)
  
  ##-************第二部分：清洗model_name*************************##
  deal_data_input<-dealFun_brandseries(yck_czb)
  deal_data_input$model_price<-as.numeric(as.character(deal_data_input$model_price))
  #--停用词清洗--#-词语描述归一-
  output_data<-fun_stopWords(deal_data_input)
  qx_czb<-data.frame(X=deal_data_input$car_id,output_data)
  #清洗多余空格
  qx_czb<-trim(qx_czb)
  qx_czb$car_model_name<-gsub(" +"," ",qx_czb$car_model_name)
  qx_czb<-sapply(qx_czb,as.character) %>% as.data.frame(stringsAsFactors=F)
  for (i in 1:dim(qx_czb)[2]) {
    qx_czb[,i][which(is.na(qx_czb[,i]))]<-""
  }
  qx_czb$X<-as.integer(as.character(qx_czb$X))
  #*************第三大章：数据匹配###调用函数计算结果列表
  che3001<-che300
  names(che300)<-names(qx_czb)
  data_input<-che300
  che300<-qx_czb
  names(che300)<-names(che3001)
  list_result<-fun_match_result(che300,data_input)
  #新增
  che300$car_id<-as.character(che300$car_id)
  che300$car_year<-as.numeric(as.character(che300$car_year))
  che300$car_price<-as.numeric(as.character(che300$car_price))
  return_db<-list_result$return_db%>%dplyr::filter(match_des=='right')%>%
    dplyr::select(id_che300=id_data_input,input_idname=id_che300)%>%unique()
  if(input_idname=='id_autohome'){
    return_db$input_idname<-as.character(return_db$input_idname)
    return_db<-dplyr::left_join(return_db,che3001,by=c('id_che300'='car_id')) %>% 
      dplyr::select(id_che300,input_idname,c_car_year=car_year,c_car_price=car_price)
    return_db<-dplyr::left_join(return_db,che300,by=c('input_idname'='car_id')) %>% 
      dplyr::select(id_che300,input_idname,c_car_year,car_year,c_car_price,car_price) %>% 
      dplyr::filter(round(abs(c_car_year+c_car_price-car_year-car_price),1)==0) %>% 
      group_by(input_idname) %>% dplyr::mutate(match_type_ah=ifelse(n()==1,1,2),is_only_ah=ifelse(min(id_che300)-id_che300==0,1,0)) %>% 
      as.data.frame() %>% dplyr::select(id_che300,input_idname,match_type_ah,is_only_ah)
  }
  #返回关联
  id_benchmark$id_che300<-as.character(id_benchmark$id_che300)
  id_z$id_che300<-as.character(id_z$id_che300)
  return_db$id_che300<-as.character(return_db$id_che300)
  id_z<-left_join(id_z,id_benchmark,by="id_che300")
  result1<-id_z%>%dplyr::filter(is.na(input_idname)==FALSE)
  result2<-id_z%>%dplyr::filter(is.na(input_idname)==T)%>%dplyr::select(id_che300)
  result_output<-rbind(result1,left_join(result2,return_db,by="id_che300"))
  write.csv(result_output,paste0(local_file,"/file/out_",input_idname,".csv"),row.names = F)
  return(1)
}

if(weekdays(Sys.Date())=='星期四'){
  #执行函数，异常邮件抛出#
  input_df<-data.frame(input_tablename=c('config_autohome_major_info_tmp','config_chezhibao_major_info','config_souhu_major_info','config_yiche_major_info'),
                       input_idname=c('id_autohome','id_czb','id_souhu','id_yiche'),stringsAsFactors = F)
  for (i in 1:nrow(input_df)) {
    if(tryCatch({fun_plat_vdatabase(as.character(input_df[i,1]),as.character(input_df[i,2]))},error=function(e){0},finally={0})!=1){
      fun_mailsend("车型库匹配异常config_platid",paste0(as.character(input_df[1,1]),'车型库匹配失败'))}
  }
  
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  id_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)%>%dplyr::select(id_che300=car_id)
  dbDisconnect(loc_channel)
  id_autohome<-read.csv(paste0(local_file,"/file/out_id_autohome.csv"))
  id_souhu<-read.csv(paste0(local_file,"/file/out_id_souhu.csv"))
  id_yiche<-read.csv(paste0(local_file,"/file/out_id_yiche.csv"))
  id_czb<-read.csv(paste0(local_file,"/file/out_id_czb.csv"))
  id_result<-left_join(left_join(left_join(left_join(id_che300,id_autohome,by="id_che300"),id_souhu,by="id_che300"),id_yiche,by="id_che300"),id_czb,by="id_che300")%>%unique()
  for (i in 1:dim(id_result)[2]) {
    id_result[,i][which(is.na(id_result[,i]))]<-0
  }
  ##入库
  fun_mysqlload_add_upd(local_file,local_defin_yun,id_result,'config_plat_id_match')
}