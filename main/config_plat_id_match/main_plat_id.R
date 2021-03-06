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
#local_file<-'//User-20170720ma/yck_wash_model_new'
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/matchFun_vdatabase.R"),echo=TRUE,encoding="utf-8")
local_defin_yun<-fun_mysql_config_up()$local_defin_yun
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
#out_rrc<<- read.csv(paste0(local_file,"/config/config_file/out_rrc.csv"),header = T,sep = ",")
rm_series_rule<<-fun_mysqlload_query(local_defin_yun,"SELECT * FROM config_reg_series_rule;")
che300<<-fun_mysqlload_query(local_defin_yun,"SELECT * FROM analysis_che300_cofig_info;")
id_z<<-che300 %>% dplyr::select(id_che300=car_id)

#input_tablename<-'config_autoowner_major_info_tmp'
#input_name<-'autoowner'

fun_plat_vdatabase<-function(input_tablename,input_name){
  input_idname<-paste0('id_',input_name)
  input_matchname<-paste0('match_type_',input_name)
  input_onlyname<-paste0('is_only_',input_name)
  yck_czb<-fun_mysqlload_query(local_defin_yun,paste0("SELECT model_id car_id,brand_name,series_name,model_name model_name_t,model_price,model_year,
                             liter,auto car_auto,discharge_standard,series_id FROM ",input_tablename," a;"))
  id_benchmark<-fun_mysqlload_query(local_defin_yun,paste0("SELECT id_che300,",input_idname,',',input_matchname,',',input_onlyname," FROM config_plat_id_match WHERE ",input_matchname," NOT IN(0,4);"))
  match_seriesid<-fun_mysqlload_query(local_defin_yun,paste0("SELECT DISTINCT a.series_id,c.brand_name temp_brand,c.series_name temp_series FROM ",input_tablename," a
                                INNER JOIN config_plat_id_match b ON a.model_id=b.",input_idname,"
                                INNER JOIN config_vdatabase_yck_major_info c ON b.id_che300=c.model_id GROUP BY series_id;"))
  id<-data.frame(car_id=setdiff(yck_czb$car_id,id_benchmark[,input_idname]))
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
  return_db<-list_result$return_db%>%dplyr::filter(match_des=='right') %>%
    dplyr::select(id_che300=id_data_input,input_idname=id_che300) %>% unique()
  return_db$input_idname<-as.character(return_db$input_idname)
  return_db<-dplyr::left_join(return_db,che3001,by=c('id_che300'='car_id')) %>% 
    dplyr::select(id_che300,input_idname,c_car_year=car_year,c_car_price=car_price)
  return_db<-dplyr::left_join(return_db,che300,by=c('input_idname'='car_id')) %>% 
    dplyr::select(id_che300,input_idname,c_car_year,car_year,c_car_price,car_price) %>% 
    dplyr::filter(round(abs(c_car_year+c_car_price-car_year-car_price),1)==0) %>% 
    group_by(input_idname) %>% dplyr::mutate(match_type=ifelse(n()==1,1,2),is_only=ifelse(min(id_che300)-id_che300==0,1,0)) %>% 
    as.data.frame() %>% dplyr::select(id_che300,input_idname,match_type,is_only)
  
  ##对于重复匹配的再次筛选（迭代-后期可整合）
  return_db_repeat<-list_result$return_db%>%dplyr::filter(match_des=='repeat')%>%
    dplyr::select(id_che300=id_data_input,input_idname=id_che300)%>%unique()
  return_db_repeat$input_idname<-as.character(return_db_repeat$input_idname)
  return_db_repeat<-dplyr::left_join(return_db_repeat,che3001,by=c('id_che300'='car_id')) %>% 
    dplyr::select(id_che300,input_idname,c_car_year=car_year,c_car_price=car_price,c_car_auto=car_auto,c_qx_name1=qx_name1,c_car_site=car_site)
  return_db_get1<-dplyr::left_join(return_db_repeat,che300,by=c('input_idname'='car_id')) %>% 
    dplyr::select(id_che300,input_idname,c_car_year,car_year,c_car_price,car_price,c_car_auto,car_auto,c_qx_name1,qx_name1,c_car_site,car_site) %>% 
    dplyr::filter(round(abs(c_car_year+c_car_price-car_year-car_price),1)==0,c_car_auto==car_auto,c_qx_name1==qx_name1,c_car_site==car_site) %>% 
    dplyr::group_by(id_che300) %>% dplyr::filter(n()==1) %>% as.data.frame() %>% 
    dplyr::group_by(input_idname) %>% dplyr::mutate(match_type=ifelse(n()==1,1,2),is_only=ifelse(min(id_che300)-id_che300==0,1,0)) %>% 
    as.data.frame() %>% dplyr::select(id_che300,input_idname,match_type,is_only)
  return_db_repeat<-inner_join(return_db_repeat,data.frame(id_che300=unique(setdiff(return_db_repeat$id_che300,return_db_get1$id_che300))),by='id_che300')
  return_db_get2<-dplyr::left_join(return_db_repeat,che300,by=c('input_idname'='car_id')) %>% 
    dplyr::select(id_che300,input_idname,c_car_year,car_year,c_car_price,car_price,c_car_auto,car_auto,c_qx_name1,qx_name1,c_car_site,car_site) %>% 
    dplyr::filter(round(abs(c_car_year+c_car_price-car_year-car_price),1)==0,c_car_site==car_site) %>% 
    dplyr::group_by(id_che300) %>% dplyr::filter(n()==1) %>% as.data.frame() %>% 
    dplyr::group_by(input_idname) %>% dplyr::mutate(match_type=ifelse(n()==1,1,2),is_only=ifelse(min(id_che300)-id_che300==0,1,0)) %>% 
    as.data.frame() %>% dplyr::select(id_che300,input_idname,match_type,is_only)
  return_db_repeat<-inner_join(return_db_repeat,data.frame(id_che300=unique(setdiff(return_db_repeat$id_che300,return_db_get2$id_che300))),by='id_che300')
  return_db_get3<-dplyr::left_join(return_db_repeat,che300,by=c('input_idname'='car_id')) %>% 
    dplyr::select(id_che300,input_idname,c_car_year,car_year,c_car_price,car_price,c_car_auto,car_auto,c_qx_name1,qx_name1,c_car_site,car_site) %>% 
    dplyr::filter(round(abs(c_car_year+c_car_price-car_year-car_price),1)==0) %>% 
    dplyr::group_by(id_che300) %>% dplyr::filter(n()==1) %>% as.data.frame() %>% 
    dplyr::group_by(input_idname) %>% dplyr::mutate(match_type=ifelse(n()==1,1,2),is_only=ifelse(min(id_che300)-id_che300==0,1,0)) %>% 
    as.data.frame() %>% dplyr::select(id_che300,input_idname,match_type,is_only)
  return_db<-rbind(return_db,rbind(rbind(return_db_get1,return_db_get2),return_db_get3))
  #返回关联
  names(return_db)<-c("id_che300",input_idname,input_matchname,input_onlyname)
  id_benchmark$id_che300<-as.character(id_benchmark$id_che300)
  id_z$id_che300<-as.character(id_z$id_che300)
  return_db$id_che300<-as.character(return_db$id_che300)
  id_z<-left_join(id_z,id_benchmark,by="id_che300")
  result1<-id_z[!is.na(id_z[,input_idname]),]
  result2<-id_z[is.na(id_z[,input_idname]),]%>%dplyr::select(id_che300)
  result_output<-rbind(result1,left_join(result2,return_db,by="id_che300"))
  write.csv(result_output,paste0(local_file,"/file/out_",input_idname,".csv"),row.names = F)
  return(1)
}
if(weekdays(Sys.Date())=='星期四'){
  #执行函数，异常邮件抛出#
  input_df<-data.frame(input_tablename=c('config_autohome_major_info_tmp','config_chezhibao_major_info','config_souhu_major_info',
                                         'config_yiche_major_info','config_che58_major_info','config_youxin_major_info_tmp',
                                         'config_autoowner_major_info_tmp'),
                       input_name=c('autohome','czb','souhu','yiche','che58','youxin','autoowner'),stringsAsFactors = F)
  for (i in 1:nrow(input_df)) {
    if(tryCatch({fun_plat_vdatabase(as.character(input_df[i,1]),as.character(input_df[i,2]))},error=function(e){0},finally={0})!=1){
      fun_mailsend("车型库匹配异常config_platid",paste0(as.character(input_df[i,1]),'车型库匹配失败'))}
  }
  id_che300<-fun_mysqlload_query(local_defin_yun,"SELECT * FROM analysis_che300_cofig_info;") %>%dplyr::select(id_che300=car_id)
  id_autohome<-read.csv(paste0(local_file,"/file/out_id_autohome.csv"))
  id_souhu<-read.csv(paste0(local_file,"/file/out_id_souhu.csv"))
  id_yiche<-read.csv(paste0(local_file,"/file/out_id_yiche.csv"))
  id_czb<-read.csv(paste0(local_file,"/file/out_id_czb.csv"))
  id_che58<-read.csv(paste0(local_file,"/file/out_id_che58.csv"))
  id_youxin<-read.csv(paste0(local_file,"/file/out_id_youxin.csv"))
  id_autoowner<-read.csv(paste0(local_file,"/file/out_id_autoowner.csv"))
  id_result<-left_join(left_join(left_join(left_join(left_join(left_join(left_join(id_che300,id_autohome,by="id_che300"),id_souhu,by="id_che300"),id_yiche,by="id_che300"),
                          id_czb,by="id_che300"),id_che58,by="id_che300"),id_youxin,by="id_che300"),id_autoowner,by="id_che300") %>% unique()
  for (i in 1:dim(id_result)[2]) {
    id_result[,i][which(is.na(id_result[,i]))]<-0
  }
  ##入库
  fun_mysqlload_add_upd(local_file,local_defin_yun,id_result,'config_plat_id_match')
}


