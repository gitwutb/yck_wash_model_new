##-------------每周一次增量更新-----------##
##---本代码处理汽车之家车型库---#
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
local_file<-gsub("(\\/main|\\/bat|\\/main_local).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
#local_file<-'//User-20170720ma/yck_wash_model_new'
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/matchFun_vdatabase.R"),echo=TRUE,encoding="utf-8")
local_defin_yun<-fun_mysql_config_up()$local_defin_yun
local_defin<-fun_mysql_config_up()$local_defin
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
out_rrc<<- read.csv(paste0(local_file,"/config/config_file/out_rrc.csv"),header = T,sep = ",")
##汽车之家车型库处理
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id car_id,brand_name,series_name,model_name model_name_t,model_price,model_year,
                             liter,auto car_auto,discharge_standard,series_id FROM config_autohome_major_info_tmp a;"),-1)
match_seriesid<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT a.series_id,c.brand_name temp_brand,c.series_name temp_series FROM config_autohome_major_info_tmp a
                                    INNER JOIN (SELECT autohome_seriesid,MIN(che300_seriesid) che300_seriesid from config_series_match_autohome GROUP BY autohome_seriesid) b ON a.series_id=b.autohome_seriesid
                                    INNER JOIN (SELECT DISTINCT series_id,series_group_name,series_name,brand_name from  config_vdatabase_yck_major_info) c ON b.che300_seriesid=c.series_id;"),-1)
rm_series_rule<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
dbDisconnect(loc_channel)
yck_czb<-left_join(yck_czb,match_seriesid,by='series_id')
yck_czb$brand_name[!is.na(yck_czb$temp_brand)]<-yck_czb$temp_brand[!is.na(yck_czb$temp_brand)]
yck_czb$series_name[!is.na(yck_czb$temp_brand)]<-yck_czb$temp_series[!is.na(yck_czb$temp_brand)]
yck_czb<-yck_czb %>% dplyr::select(-series_id,-temp_brand,-temp_series)

##--清洗品牌、车系*************************##
deal_data_input<-dealFun_brandseries(yck_czb)
deal_data_input$model_price<-as.numeric(as.character(deal_data_input$model_price))
#--停用词清洗--#-词语描述归一-
output_data<-fun_stopWords(deal_data_input)
qx_czb<-data.frame(car_id=deal_data_input$car_id,output_data)
#清洗多余空格
qx_czb<-trim(qx_czb)
qx_czb$car_model_name<-gsub(" +"," ",qx_czb$car_model_name)
qx_czb<-sapply(qx_czb,as.character) %>% as.data.frame(stringsAsFactors=F)
for (i in 1:dim(qx_czb)[2]) {
  qx_czb[,i][which(is.na(qx_czb[,i]))]<-""
}
qx_czb$car_id<-as.integer(as.character(qx_czb$car_id))
##
che300<-qx_czb
id_z<<-che300 %>% dplyr::select(id_che300=car_id)
fun_plat_match_firstauto<-function(){
  input_tablename<-'config_firstauto_major_info'
  input_name<-'firstauto'
  input_idname<-paste0('id_',input_name)
  input_matchname<-paste0('match_type_',input_name)
  input_onlyname<-paste0('is_only_',input_name)
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id car_id,brand_name,series_name,model_name model_name_t,model_price,model_year,
                               liter,auto car_auto,discharge_standard,series_id FROM config_firstauto_major_info a;"),-1)
  id_benchmark<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT id_autohome,",input_idname,',',input_matchname,',',input_onlyname," FROM config_plat_match_autohome_firstauto WHERE ",input_matchname,"!=0;")),-1)
  dbDisconnect(loc_channel)
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
  return_db$c_car_year<-as.numeric(return_db$c_car_year)
  return_db$c_car_price<-as.numeric(return_db$c_car_price)
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
  return_db_repeat$c_car_year<-as.numeric(return_db_repeat$c_car_year)
  return_db_repeat$c_car_price<-as.numeric(return_db_repeat$c_car_price) 
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
  id_result<-left_join(id_che300,result_output,by="id_che300") %>% unique()
  id_result<-id_result %>% dplyr::select(id_autohome=id_che300,id_firstauto,match_type_firstauto,is_only_firstauto)
  fun_mysqlload_add_upd(local_file,local_defin_yun,id_result,'config_plat_match_firstauto')
  return(1)
}


if(weekdays(Sys.Date())=='星期四'){
   if(tryCatch({fun_plat_match_firstauto()},error=function(e){0},finally={0})!=1){
  fun_mailsend("车源车型与第一车网匹配异常",'config_plat_match_firstauto匹配失败')}
  }

####更新匹配表：yck_itp_firstauto_idmatch
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
config_up<-dbFetch(dbSendQuery(loc_channel,"SELECT s.autohome_id id_autohome,s.id_firstauto,d.brand_name brand_autohome,e.brand_name brand_firstauto,d.series_name series_autohome,e.series_name series_firstauto,
                              d.model_year year_autohome,e.model_year year_firstauto,d.model_name model_autohome,e.model_name model_firstauto,d.model_price price_autohome,e.model_price price_firstauto,
                              d.discharge_standard standard_autohome,e.discharge_standard standard_firstauto from 
                              (SELECT DISTINCT a.autohome_id,c.id_firstauto from yck_tableau_it_regular a LEFT JOIN yck_itp_firstauto_idmatch b  on a.autohome_id=b.id_autohome
                              INNER JOIN config_plat_match_firstauto c on a.autohome_id=c.id_autohome where b.id_firstauto is NULL) s
                              INNER JOIN config_autohome_major_info_tmp d on s.autohome_id=d.model_id
                              LEFT JOIN config_firstauto_major_info e on s.id_firstauto=e.model_id;"),-1)
dbDisconnect(loc_channel)




fun_mailsend_check<-function(input_subject,input_body){
  send.mail(from = "2579104076@qq.com",
            to = c("2579104076@qq.com"),
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
n<-nrow(config_up)
if(n>0){
  fun_mailsend_check('新增车源车型匹配校验',paste0('新增',n,'款车，请进行匹配校验!'))
}

##1.检查添加到config_plat_match_firstauto后执行
# config_down<-config_up%>%filter(config_up$id_firstauto!=0) %>%dplyr::select(id_autohome,id_firstauto)
# fun_mysqlload_add_upd(local_file,local_defin_yun,config_down,'yck_itp_firstauto_idmatch')

##csv文件检查
# file_path<-c("D:/Users/Desktop/item")
# write.csv(config_up,paste0(file_path,"/config_up.csv"),row.names = F,fileEncoding = "UTF-8",quote = F)
