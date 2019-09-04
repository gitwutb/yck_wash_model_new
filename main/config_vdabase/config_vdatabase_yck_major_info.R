##-------------每周一次增量更新-----------##
##---本代码处理车300车型库构建YCK车型库---#
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
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
local_defin_yun<<-fun_mysql_config_up()$local_defin_yun
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
##车型库ID自增处理
fun_config_vdatabase_info<-function(){
  ####--车型库构建：品牌ID增加--####
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  brand_add<-dbFetch(dbSendQuery(loc_channel,"SELECT a.Initial,a.brandid,a.brand_name FROM
        (SELECT DISTINCT Initial,brandid,brand_name FROM config_che300_major_info) a 
        LEFT JOIN config_vdatabase_yck_brand b ON a.brandid=b.brandid
        WHERE yck_brandid IS NULL;"),-1)
  brand_all<-dbFetch(dbSendQuery(loc_channel,"SELECT a.Initial,a.brandid,a.brand_name,yck_brandid FROM
        (SELECT DISTINCT Initial,brandid,brand_name FROM config_che300_major_info) a 
        INNER JOIN config_vdatabase_yck_brand b ON a.brandid=b.brandid;"),-1)
  dbDisconnect(loc_channel)
  if(nrow(brand_add)>0){
    brand_all$yck_brandid<-as.numeric(gsub("[a-zA-Z]","",brand_all$yck_brandid))
    brand_Initial_max<-brand_all %>% group_by(Initial) %>% summarise(maxb=max(as.integer(yck_brandid))) %>%
      ungroup() %>% as.data.frame()
    brand_add<-left_join(brand_add,brand_Initial_max,by=c('Initial'))
    brand_add$maxb[is.na(brand_add$maxb)]<-0
    brand_add<-brand_add %>% group_by(Initial) %>% mutate(yck_brandid=paste0(Initial,maxb+as.integer(factor(brand_name)))) %>%
      as.data.frame() %>% dplyr::select(brandid,brand_name,yck_brandid) %>% mutate(car_country='无')
    fun_mysqlload_add(local_file,local_defin_yun,brand_add,"config_vdatabase_yck_brand")
  }
  
  ####--车型库构建：车系ID增加--####
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  series_add<-dbFetch(dbSendQuery(loc_channel,"SELECT a.series_id,a.series_group_name,a.series_name,yck_brandid FROM
        (SELECT DISTINCT brandid,series_id,series_group_name,series_name FROM config_che300_major_info) a 
        LEFT JOIN config_vdatabase_yck_series b ON a.series_id=b.series_id
        INNER JOIN config_vdatabase_yck_brand c ON a.brandid=c.brandid
        WHERE yck_seriesid IS NULL;"),-1)
  series_all<-dbFetch(dbSendQuery(loc_channel,"SELECT yck_brandid,yck_seriesid FROM
        (SELECT DISTINCT brandid,series_id,series_name FROM config_che300_major_info) a 
        INNER JOIN config_vdatabase_yck_series b ON a.series_id=b.series_id
        INNER JOIN config_vdatabase_yck_brand c ON a.brandid=c.brandid;"),-1)
  dbDisconnect(loc_channel)
  if(nrow(series_add)>0){
    series_add$series_group_name[grep('进口',series_add$series_name)]<-
      paste0('进口',series_add$series_group_name[grep('进口',series_add$series_name)])
    series_add$series_group_name<-gsub('进口进口进口|进口进口','进口',series_add$series_group_name)
    series_add$series_name<-gsub('\\(进口)','',series_add$series_name)
    series_add$series_name<-gsub('欧尚COSMOS\\(科尚\\)','欧尚COSMOS-科尚',series_add$series_name)
    series_add$series_name<-toupper(series_add$series_name)
    is_import<-str_extract(series_add$series_group_name,'进口')
    is_import[is_import=='进口']<-1
    is_import[which(is.na(is_import))]<-0
    
    series_all$yck_seriesid<-as.numeric(gsub('.*[a-zA-Z]','',series_all$yck_seriesid))
    series_all<-series_all%>% group_by(yck_brandid) %>% summarise(maxs=max(yck_seriesid)) %>% as.data.frame()
    series_add<-left_join(series_add,series_all,by=c('yck_brandid'))
    series_add$maxs[is.na(series_add$maxs)]<-0
    
    series_add<-series_add %>% group_by(yck_brandid) %>% mutate(yck_seriesid=paste0(yck_brandid,'S',as.integer(maxs)+as.integer(factor(series_name)))) %>%
      as.data.frame() %>% dplyr::select(series_id,series_group_name,series_name,yck_seriesid) %>%
      mutate(is_import=is_import) %>% mutate(car_level='无',is_green='无')
    fun_mysqlload_add(local_file,local_defin_yun,series_add,"config_vdatabase_yck_series")
  }
  
  ####--车型库构建：车型ID增加--####
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  model_add<-dbFetch(dbSendQuery(loc_channel,"SELECT a.model_id,a.Initial,a.brandid,d.yck_brandid,a.series_id,c.yck_seriesid FROM config_che300_major_info a 
        LEFT JOIN config_vdatabase_yck_model b ON a.model_id=b.model_id
        LEFT JOIN config_vdatabase_yck_series c ON a.series_id=c.series_id
        LEFT JOIN config_vdatabase_yck_brand d ON a.brandid=d.brandid
        WHERE yck_modelid IS NULL;"),-1)
  model_all<-dbFetch(dbSendQuery(loc_channel,"SELECT yck_seriesid,yck_modelid FROM config_vdatabase_yck_model a
        INNER JOIN config_che300_major_info b ON a.model_id=b.model_id
        INNER JOIN config_vdatabase_yck_series c ON b.series_id=c.series_id;"),-1)
  dbDisconnect(loc_channel)
  if(nrow(model_add)>0){
    model_all$yck_modelid<-as.numeric(gsub('.*[a-zA-Z]','',model_all$yck_modelid))
    model_all<-model_all%>% group_by(yck_seriesid) %>% summarise(maxs=max(yck_modelid)) %>% as.data.frame()
    model_add<-left_join(model_add,model_all,by=c('yck_seriesid'))
    model_add$maxs[is.na(model_add$maxs)]<-0
    model_add<-model_add %>% group_by(yck_seriesid) %>% mutate(yck_modelid=paste0(yck_seriesid,'M',as.integer(maxs)+as.integer(factor(model_id)))) %>%
      as.data.frame() %>% dplyr::select(yck_modelid,model_id)
    fun_mysqlload_add_upd(local_file,local_defin_yun,model_add,"config_vdatabase_yck_model")
  }
  
  ####--车型库构建：车型库信息更新--####
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,"REPLACE INTO config_vdatabase_yck_major_info SELECT a.yck_modelid,a.model_id,b.Initial,
                  b.brandid,c.yck_brandid,c.brand_name,d.series_group_name,b.series_id,d.yck_seriesid,d.series_name,
                  b.model_name,b.short_name,b.model_price,b.model_year,b.auto,b.liter,b.discharge_standard,b.max_reg_year,
                  b.min_reg_year,d.car_level,b.seat_number,b.hl_configs,b.hl_configc,d.is_green,d.is_import
                FROM config_vdatabase_yck_model a
                INNER JOIN config_che300_major_info b ON a.model_id=b.model_id
                INNER JOIN config_vdatabase_yck_brand c ON b.brandid=c.brandid
                INNER JOIN config_vdatabase_yck_series d ON b.series_id=d.series_id
            WHERE d.is_green !='无' AND d.car_level!='无' AND b.auto in('手动','自动','电动')")
  dbSendQuery(loc_channel,"UPDATE config_vdatabase_yck_major_info SET is_green=2 WHERE model_id in 
              (select model_id from config_che300_detail_info where eng_fuel_type REGEXP '增程式|插电式|油电')")
  dbSendQuery(loc_channel,"UPDATE config_vdatabase_yck_major_info SET is_green=0 WHERE model_id in 
              (select model_id from config_che300_detail_info where eng_fuel_type REGEXP '汽油')")
  dbSendQuery(loc_channel,"UPDATE config_vdatabase_yck_major_info SET is_green=1 WHERE model_id in 
              (select model_id from config_che300_detail_info where eng_fuel_type REGEXP '纯电动|电力')")
  dbSendQuery(loc_channel,"UPDATE config_vdatabase_yck_major_info SET auto='自动' WHERE is_green=2 AND auto='电动'")
  dbDisconnect(loc_channel)
  return(1)
}
##处理后的车300车型库
fun_analysis_che300_cofig<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  data_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id,brand_name,series_group_name,series_name,short_name model_name,
        model_price,model_year,auto,liter,discharge_standard FROM config_vdatabase_yck_major_info;"),-1)
  dbDisconnect(loc_channel)
  
  #-----数据转换名称------
  data_input<-data_che300
  ##--------------------停用词清洗--------------------
  output_data<-fun_stopWords(data_input)
  qx_che300<-data.frame(data_input$model_id,output_data)
  #清洗多余空格
  qx_che300<-trim(qx_che300)
  qx_che300$car_model_name<-gsub(" +"," ",qx_che300$car_model_name)
  qx_che300<-sapply(qx_che300,as.character) %>% as.data.frame(stringsAsFactors=F)
  for (i in 1:dim(qx_che300)[2]) {
    qx_che300[,i][-grep("",qx_che300[,i])]<-""
  }
  fun_mysqlload_add_upd(local_file,local_defin_yun,qx_che300,'analysis_che300_cofig_info')
}
##附属表
fun_config_reg_series_rule<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  data_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT car_name,car_series1 FROM analysis_che300_cofig_info;"),-1)
  dbDisconnect(loc_channel)
  #-----数据转换名称------
  data_input<-data_che300
  ##car_name---------得到品牌名-系列名-------
  car_name<-toupper(data_input$car_name)
  car_series1<-toupper(data_input$car_series1)
  car_series1<-gsub("-进口","",car_series1)
  
  ###########---------构建中间系列qx_series3----############
  car_name<-gsub("\\·|\\?","",car_name)
  seriesFun<-function(i){
    sub(car_name[i],"",car_series1[i])
  }
  qx_series_des<-unlist(lapply(1:length(car_series1),seriesFun))
  qx_series_des<-trim(qx_series_des)
  qx_series_all<-paste(car_name,qx_series_des,sep = "")
  a<-which(nchar(qx_series_des)==0)
  qx_series_des[a]<-car_name[a]
  
  qx_series_des<-gsub(" ","",qx_series_des)
  qx_series_all<-gsub(" ","",qx_series_all)
  
  ce_data<-unique(data.frame(name=car_name,series=car_series1,series_t=car_series1,qx_series_all,qx_series_des))
  ce_data<-ce_data[order(ce_data[,1],ce_data[,2],decreasing=T),]
  ce_data$series_t<-gsub(" ","",ce_data$series_t)
  ##补齐
  n<-nrow(ce_data)
  rule_name<-as.character(unique(ce_data$name))
  rule_name<-c(rule_name,rep(rule_name[length(rule_name)],n-length(rule_name)))
  #series
  rule_series<-as.character(unique(ce_data$series))
  rule_series<-c("福田风景","别克赛欧",rule_series[order(rule_series,decreasing=T)])%>%unique()
  rule_series<-c(rule_series,"MINI","中华")
  rule_series<-c(rule_series,rep(rule_series[length(rule_series)],n-length(rule_series)))
  ce_data<-data.frame(id=c(1:n),ce_data,rule_name,rule_series)
  
  write.csv(ce_data,paste0(local_file,"/file/reg_series_rule.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,"TRUNCATE TABLE config_reg_series_rule")
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/file/reg_series_rule.csv",sep=""),"'",
                                 " INTO TABLE config_reg_series_rule CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}

if(weekdays(Sys.Date())=='星期四'){
  if(tryCatch({fun_config_vdatabase_info()},error=function(e){0},finally={0})!=1){
    fun_mailsend("main_che300执行错误",'车型库ID自增处理异常')
  }else{
    if(tryCatch({fun_analysis_che300_cofig()},error=function(e){0},finally={0})!=1){
      fun_mailsend("main_che300执行错误",'fun_analysis_che300_cofig')}
    if(tryCatch({fun_config_reg_series_rule()},error=function(e){0},finally={0})!=1){
      fun_mailsend("main_che300执行错误",'fun_config_reg_series_rule')}
  }
}