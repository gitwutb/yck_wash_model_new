##搜狐汽车车系与车300车系ID匹配add（main_che300处理之后执行）
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
#help(package="dplyr")
#读取数据
library(RMySQL)
local_file<-gsub("(\\/bat|\\/main).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
local_defin_yun<-fun_mysql_config_up()$local_defin_yun
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")


loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
yck_czw12365<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id car_id,brand_name,series_name,CONCAT(brand_name,series_name) model_name,model_price,series_id car_auto,series_id FROM config_souhu_major_info;"),-1)
id_match<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT id_souhu model_id FROM config_plat_id_match WHERE id_souhu!=0;"),-1)
id_match1<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT c.series_id series_id_sohu FROM config_plat_id_match a 
                          INNER JOIN config_vdatabase_yck_major_info b ON a.id_che300=b.model_id
                          INNER JOIN config_souhu_major_info c ON a.id_souhu=c.model_id
                          WHERE id_souhu!=0
                          UNION SELECT DISTINCT series_id_sohu FROM config_series_id_match;"),-1)
rm_series_rule<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
config_series_levels<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT brand_name brand_c300,series_name series_c300,series_id series_id_c300 FROM config_vdatabase_yck_major_info;"),-1)
dbDisconnect(loc_channel)
yck_not_have<-data.frame(car_id=setdiff(yck_czw12365$car_id,id_match$model_id))
yck_czw12365<-merge(yck_czw12365,yck_not_have,by="car_id")
yck_not_have<-data.frame(series_id=setdiff(unique(yck_czw12365$series_id),id_match1$series_id_sohu))
yck_czw12365<-merge(yck_czw12365,yck_not_have,by="series_id") %>% dplyr::select(-series_id)


######################------第一部分：得到车的品牌及车系-------#################
##临时车名
input_test1<-yck_czw12365
input_test1$brand_name<-gsub(" ","",input_test1$brand_name)
input_test1$series_name<-gsub(" ","",input_test1$series_name)
input_test1$model_name<-gsub(" ","",input_test1$model_name)
#input_test1清洗
input_test1$series_name<-gsub("III|Ⅲ","-3",input_test1$series_name)
input_test1$series_name<-gsub("Ⅱ|II","-2",input_test1$series_name)
input_test1$brand_name<-fun_normalization(input_test1$brand_name)
input_test1$series_name<-fun_normalization(input_test1$series_name)
input_test1$model_name<-fun_normalization(input_test1$model_name)
input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
input_test1$car_id<-as.integer(input_test1$car_id)
rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
rm_series_rule$series<-as.character(rm_series_rule$series)

###input_test2<-input_test1    input_test1<-input_test2
###----------------前期准备：提取准确的brand和series-----------
brand_name<-str_extract(input_test1$brand_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
brand_name[which(is.na(brand_name))]<-input_test1$brand_name[which(is.na(brand_name))]
linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
series_name[which(is.na(series_name))]<-input_test1$series_name[which(is.na(series_name))]
linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
linshi[which(is.na(linshi))]<-""
series_name<-paste(series_name,linshi,sep = "")
####------重新将新的brand及series替换------##
input_test1$brand_name<-brand_name
input_test1$series_name<-series_name
input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
input_test1$series_name[grep("COUNTRYMAN",input_test1$model_name)]<-"COUNTRYMAN"

###----------------第一步：分离-----------#######
car_name_info<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
input_test1<-inner_join(input_test1,a2,by="car_id")
#全称匹配到车300
a1$qx_series_all<-as.character(a1$qx_series_all)
a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,car_auto)
###----------------第二步：对剩余部分进行全称及系列名匹配-----------
seriesFun<-function(i){
  sub(input_test1$brand_name[i],"",input_test1$series_name[i])
}
qx_series_des<-unlist(lapply(1:length(input_test1$series_name),seriesFun))
qx_series_des<-trim(qx_series_des)
qx_series_all<-paste(input_test1$brand_name,qx_series_des,sep = "")
qx_series_des[which(nchar(qx_series_des)==0)]<-input_test1$brand_name[which(nchar(qx_series_des)==0)]
input_test1<-data.frame(input_test1,qx_series_all)
##############--------匹配全称------#########
input_test1$qx_series_all<-as.character(input_test1$qx_series_all)
#匹配到全称
a2<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,car_auto)
###----------------第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）-----------
a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
#全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
a3$series_t<-as.character(a3$series_t)
rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,car_auto)
##-----------------第四步：未匹配上a4-----------########
a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
if(nrow(a4)==0){
  data_input_0<-rbind(a1,a2,a3)
}else{
  a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
  a4$name<-a4$brand_name
  a4$series<-a4$series_name
  data_input_0<-rbind(a1,a2,a3,a4)
}
########----组合所有car_id---###########
data_input_0<-rbind(a1,a2,a3) ##添加
data_input_0<-inner_join(data_input_0,yck_czw12365[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
  dplyr::select(brand_name=name,series_name=series,series_id_sohu=car_auto) %>% unique()

config_series_id_add<-merge(config_series_levels,data_input_0,
                            by.x=c('brand_c300','series_c300'),
                            by.y=c('brand_name','series_name')) %>% dplyr::select(series_id_300=series_id_c300,series_id_sohu) %>% unique()

#写入数据库
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
config_series_id_match<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT b.series_id series_id_300,c.series_id series_id_sohu FROM config_plat_id_match a 
                INNER JOIN config_vdatabase_yck_major_info b ON a.id_che300=b.model_id
                INNER JOIN config_souhu_major_info c ON a.id_souhu=c.model_id
                WHERE id_souhu!=0
                UNION SELECT DISTINCT series_id_300,series_id_sohu FROM config_series_id_match;"),-1)
config_series_id_match<-rbind(config_series_id_match,config_series_id_add) %>% unique()
dbWriteTable(loc_channel,'config_series_id_match',config_series_id_match,overwrite = T,row.names=F)
dbDisconnect(loc_channel)