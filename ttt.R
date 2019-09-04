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
source(paste0(local_file,"/config/config_fun/fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
local_defin_yun<<-fun_mysql_config_up()$local_defin
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")


loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
discount_rate_history_copy<-dbFetch(dbSendQuery(loc_channel,"SELECT a.id,a.model_id,a.bare_discount_rate,a.province_name,a.update_time,a.total FROM discount_rate_history_copy a;"),-1)
config_autohome_major_info_tmp<-dbFetch(dbSendQuery(loc_channel,"SELECT a.brand_id,a.brand_name,a.series_id,a.series_name,a.model_id,a.model_name,a.model_price,a.model_year FROM config_autohome_major_info_tmp a
;"),-1)
discount_rate_history1<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM discount_rate_history a;"),-1)
dbDisconnect(loc_channel)
discount_rate_history_copy$bare_discount_rate<-as.numeric(discount_rate_history_copy$bare_discount_rate)
discount_rate_history_copy<-discount_rate_history_copy %>%filter(bare_discount_rate>=60&bare_discount_rate<=110) 

discount_rate_history<-inner_join(discount_rate_history_copy,config_autohome_major_info_tmp,by='model_id') %>%
  mutate(bare_price_mean=round(as.numeric(model_price)*as.numeric(bare_discount_rate)/100,2),stat_time=update_time)%>% 
  dplyr::select(id,brand_id,brand_name,series_id,series_name,model_id,model_year,model_name,model_price,bare_price_mean,province_name,total,stat_time,update_time)
discount_rate_history$stat_time<-gsub('-28|-29|-30','-15',discount_rate_history$stat_time)
discount_rate_history$total[which(is.na(discount_rate_history$total))]<-'\\N'

discount_rate_history1<-inner_join(discount_rate_history1,config_autohome_major_info_tmp[,c('model_id','model_year')],by='model_id')

discount_rate_history<-rbind(discount_rate_history,discount_rate_history1)
fun_mysqlload_add_upd(local_file,local_defin_yun,discount_rate_history,'discount_rate_history')


loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE discount_rate_history")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(local_file,"/file/discount_rate_history.csv",sep=""),"'",
                               " INTO TABLE discount_rate_history CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
dbDisconnect(loc_channel)