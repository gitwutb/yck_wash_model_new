#清除缓存
rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
library(reshape2)
deep_local<-gsub("\\/bat|\\/main.*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
local_defin<-data.frame(user = 'root',host='192.168.0.111',password= '000000',dbname='yck-data-center',stringsAsFactors = F)
local_defin_yun<-data.frame(user = 'yckdc',host='47.106.189.86',password= 'YckDC888',dbname='yck-data-center',stringsAsFactors = F)
data_new<-Sys.Date()%>%as.character()
#步骤一
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
table.name<-dbListTables(loc_channel)
# field.name<-dbListFields(loc_channel,"")
yck_che58<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,model model_name,emission discharge_standard,gearbox car_auto FROM spider_www_58 a
                               INNER JOIN (SELECT a.id_data_input FROM analysis_match_id a where a.car_platform='che58' AND a.match_des='not') m ON a.id=m.id_data_input;"),-1)
rm_series_rule<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
dbDisconnect(loc_channel)
rm_rule<- read.csv(paste0(deep_local,"\\config\\config_file\\reg_rule.csv",sep=""),header = T,sep = ",")
out_rrc<- read.csv(paste0(deep_local,"\\config\\config_file\\out_rrc.csv",sep=""),header = T,sep = ",")
rm_che58<- read.csv(paste0(deep_local,"\\config\\config_file\\reg_che58.csv",sep=""),header = T,sep = ",")
source(paste0(deep_local,"\\config\\config_fun\\fun_stopWords.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_normalization.R",sep=""),echo=TRUE,encoding="utf-8")
yck_che58<-data.frame(car_id=yck_che58$car_id,brand_name="",series_name="",yck_che58[,-1],model_price="")
yck_che58$discharge_standard<-gsub("\\(.*","",yck_che58$discharge_standard)
yck_che58$model_name[grep("双离合|G-DCT|DCT|DSG",yck_che58$car_auto)]<-
  paste0(yck_che58$model_name[grep("双离合|G-DCT|DCT|DSG",yck_che58$car_auto)]," 双离合",sep="")

######################------第一部分：得到车的品牌及车系-------#################
##临时车名
input_test1<-yck_che58
input_test1$model_name<-gsub(" ","",input_test1$model_name)
#######优信特别添加#########
input_test1$model_name<-gsub("汽车","",input_test1$model_name)
input_test1$model_name[grep("奔驰AMG",input_test1$model_name)]<-
  paste0(gsub("AMG-|AMG","",str_extract(input_test1$model_name[grep("奔驰AMG",input_test1$model_name)],"奔驰AMG.*级")),"AMG",sep="")
linshi<-str_extract(input_test1$model_name[grep("福特猛禽F系",input_test1$model_name)],"350|150")
linshi[which(is.na(linshi))]<-"150"
input_test1$model_name[grep("福特猛禽F系",input_test1$model_name)]<-
  paste0("福特F",linshi,sep="")

#input_test1清洗
input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
input_test1$model_name<-fun_normalization(input_test1$model_name)
input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
input_test1$car_id<-as.integer(input_test1$car_id)
rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
rm_series_rule$series<-as.character(rm_series_rule$series)

###input_test2<-input_test1    input_test1<-input_test2
###----------------前期准备：提取准确的brand和series-----------
brand_name<-str_extract(input_test1$model_name,c(str_c(unique(rm_series_rule$rule_name),sep="",collapse = "|")))
brand_name[which(is.na(brand_name))]<-""
linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
series_name[which(is.na(series_name))]<-""
linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
linshi[which(is.na(linshi))]<-""
series_name<-paste(series_name,linshi,sep = "")
####------重新将新的brand及series替换------##
input_test1$brand_name<-brand_name
input_test1$series_name<-series_name
input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"


###----------------第一步：分离-----------#######
car_name_info<-str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
input_test1<-inner_join(input_test1,a2,by="car_id")
#全称匹配到车300
a1$qx_series_all<-as.character(a1$qx_series_all)
a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto)
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
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto)
###----------------第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）-----------
a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
#全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
a3$series_t<-as.character(a3$series_t)
rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
  dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto)
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
data_input_0<-inner_join(data_input_0,yck_che58[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
  dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
data_input_0$model_name<-toupper(data_input_0$model_name)
data_input_0$auto<-gsub("-／","",data_input_0$auto)
data_input_0<-data.frame(data_input_0,liter="")
#---准确性accurate
accurate<-c(length(a4),nrow(yck_che58))
rm(a1,a2,a3,a4,car_name_info,input_test1,qx_series_all,qx_series_des)
gc()



######################------第二部分：清洗model_name-------#################
#-----数据转换名称------
data_input<-data_input_0
data_input$model_price<-as.integer(data_input$model_price)
qx_name<-toupper(data_input$model_name)
qx_name<-trim(qx_name)
qx_name<-gsub(" +"," ",qx_name)
car_model_name<-data_input$model_name
##---------part1：得到年款及指导价--------------car_model_name[-grep("",car_year)]
qx_name<-gsub("2012年","2012款",qx_name)
qx_name<-gsub("-2011 款","2011款 ",qx_name)
car_year<-str_extract(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
car_year<-gsub("款","",car_year)
car_year[which(nchar(car_year)==2)]<-paste("20",car_year[which(nchar(car_year)==2)],sep = "")
loc_year<-str_locate(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
car_price<-round(data_input$model_price/10000,2)
##########################################优信年款前有其它信息(按年款拆分截成两部分)############
qx1_wutb<-str_sub(qx_name,1,loc_year[,1]-1)
qx2_wutb<-str_sub(qx_name,loc_year[,2]+1)
qx1_wutb<-gsub(" ","",qx1_wutb)
#######优信特别添加#########
qx1_wutb<-gsub("汽车","",qx1_wutb)
qx1_wutb[grep("奔驰AMG",qx1_wutb)]<-
  paste0(gsub("AMG-|AMG","",str_extract(qx1_wutb[grep("奔驰AMG",qx1_wutb)],"奔驰AMG.*级")),"AMG",sep="")
linshi<-str_extract(qx1_wutb[grep("福特猛禽F系",qx1_wutb)],"350|150")
linshi[which(is.na(linshi))]<-"150"
qx1_wutb[grep("福特猛禽F系",qx1_wutb)]<-
  paste0("福特F",linshi,sep="")
#input_test1清洗
qx1_wutb<-fun_normalization(qx1_wutb)
qx1_wutb<-gsub("昂科赛拉","昂克赛拉",qx1_wutb)
qx1_wutb<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",qx1_wutb)
qx1_wutb[grep("赛欧.*赛欧3",qx1_wutb)]<-sub("赛欧","",qx1_wutb[grep("赛欧.*赛欧3",qx1_wutb)])
car_name<-data_input$brand_name
car_series1<-data_input$series_name
car_series1<-gsub(" ","",car_series1)
seriesFun<-function(i){
  gsub(car_series1[i],"",qx1_wutb[i])
}
qx1_wutb<-unlist(lapply(1:length(car_name),seriesFun))
##3、清洗品牌
seriesFun<-function(i){
  gsub(car_name[i],"",qx1_wutb[i])
}
qx1_wutb<-unlist(lapply(1:length(car_name),seriesFun))
qx_name<-paste0(qx1_wutb," ",qx2_wutb,sep="")

#############################################################################
qx_name<-gsub("\\（","(",qx_name)
qx_name<-gsub("\\）",")",qx_name)
qx_name<-gsub("\\(进口\\)","",qx_name)
qx_name<-gsub("\\(海外\\)","",qx_name)
qx_name<-gsub("POWER DAILY","宝迪",qx_name)
qx_name<-gsub("III","Ⅲ",qx_name)
qx_name<-gsub("II","Ⅱ",qx_name)
qx_name<-gsub("—|－","-",qx_name)
qx_name<-gsub("\\·|\\?|(-|)JL465Q","",qx_name)
qx_name<-gsub("ONE\\+","ONE佳",qx_name)
qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
qx_name<-gsub("选装(包|)","佳",qx_name)
qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)

car_name<-data_input$brand_name
car_series1<-data_input$series_name
forFun<-function(i){
  sub(car_series1[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_series1),forFun))


##--------------------停用词清洗--------------------
####################################################CROSS
###-------词语描述归一-----
output_data<-fun_stopWords(data_input,qx_name)
#------------------数据保存----------------
qx_che58<-data.frame(X=data_input_0$car_id,output_data)

#清洗多余空格
qx_che58<-trim(qx_che58)
qx_che58$car_model_name<-gsub(" +"," ",qx_che58$car_model_name)
qx_che58<-sapply(qx_che58,as.character)
for (i in 1:dim(qx_che58)[2]) {
  qx_che58[,i][which(is.na(qx_che58[,i]))]<-""
}
qx_che58<-data.frame(qx_che58)
qx_che58$X<-as.integer(as.character(qx_che58$X))
#剔除不包含的车系
df_filter<- gsub('-进口','',unique(qx_che58$car_series1)) %>% as.character()
che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
#########################################################################################################
##################################################第二大章：数据匹配#####################################
source(paste0(deep_local,"\\config\\config_fun\\fun_match.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_iteration.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_match_result.R",sep=""),echo=TRUE,encoding="utf-8")
data_input<-qx_che58
##调用函数计算结果列表
list_result<-fun_match_result(che300,qx_che58)
confidence<-list_result$confidence
return_db<-list_result$return_db
match_right<-list_result$match_right
match_repeat<-list_result$match_repeat
match_not<-list_result$match_not
return_db<-data.frame(car_platform="che58",return_db)
return_db$id_che300<-as.integer(as.character(return_db$id_che300))
#########################################################################
#######################################################################





#**************************************************************************************#
########******************根据车58实际情况需要再次过滤***************###################
wutb_right<-match_right
che58_sid<-data.frame(car_id=setdiff(data_input_0$car_id,unique(wutb_right$id_data_input)))
data_input_se<-inner_join(data_input_0,che58_sid,c("car_id"="car_id"))

#####清洗垃圾信息#####
data_input<-data_input_se
data_input$model_price<-as.integer(data_input$model_price)
qx_name<-toupper(data_input$model_name)
qx_name<-trim(qx_name)
qx_name<-gsub(" +"," ",qx_name)
car_model_name<-data_input$model_name
##---------part1：得到年款及指导价--------------car_model_name[-grep("",car_year)]
qx_name<-gsub("2012年","2012款",qx_name)
qx_name<-gsub("-2011 款","2011款 ",qx_name)
car_year<-str_extract(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
car_year<-gsub("款","",car_year)
car_year[which(nchar(car_year)==2)]<-paste("20",car_year[which(nchar(car_year)==2)],sep = "")
loc_year<-str_locate(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
car_price<-round(data_input$model_price/10000,2)
##########################################优信年款前有其它信息(按年款拆分截成两部分)############
qx1_wutb<-str_sub(qx_name,1,loc_year[,1]-1)
qx2_wutb<-str_sub(qx_name,loc_year[,2]+1)
qx1_wutb<-gsub(" ","",qx1_wutb)
#######优信特别添加#########
qx1_wutb<-gsub("汽车","",qx1_wutb)
qx1_wutb[grep("奔驰AMG",qx1_wutb)]<-
  paste0(gsub("AMG-|AMG","",str_extract(qx1_wutb[grep("奔驰AMG",qx1_wutb)],"奔驰AMG.*级")),"AMG",sep="")
linshi<-str_extract(qx1_wutb[grep("福特猛禽F系",qx1_wutb)],"350|150")
linshi[which(is.na(linshi))]<-"150"
qx1_wutb[grep("福特猛禽F系",qx1_wutb)]<-
  paste0("福特F",linshi,sep="")
#input_test1清洗
qx1_wutb<-fun_normalization(qx1_wutb)
qx1_wutb<-gsub("昂科赛拉","昂克赛拉",qx1_wutb)
qx1_wutb<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",qx1_wutb)
qx1_wutb[grep("赛欧.*赛欧3",qx1_wutb)]<-sub("赛欧","",qx1_wutb[grep("赛欧.*赛欧3",qx1_wutb)])
car_name<-data_input$brand_name
car_series1<-data_input$series_name
car_series1<-gsub(" ","",car_series1)
seriesFun<-function(i){
  gsub(car_series1[i],"",qx1_wutb[i])
}
qx1_wutb<-unlist(lapply(1:length(car_name),seriesFun))
##3、清洗品牌
seriesFun<-function(i){
  gsub(car_name[i],"",qx1_wutb[i])
}
qx1_wutb<-unlist(lapply(1:length(car_name),seriesFun))
qx_name<-paste0(qx1_wutb," ",qx2_wutb,sep="")

#############################################################################
qx_name<-gsub("\\（","(",qx_name)
qx_name<-gsub("\\）",")",qx_name)
qx_name<-gsub("\\(进口\\)","",qx_name)
qx_name<-gsub("\\(海外\\)","",qx_name)
qx_name<-gsub("POWER DAILY","宝迪",qx_name)
qx_name<-gsub("III","Ⅲ",qx_name)
qx_name<-gsub("II","Ⅱ",qx_name)
qx_name<-gsub("—|－","-",qx_name)
qx_name<-gsub("\\·|\\?|(-|)JL465Q","",qx_name)
qx_name<-gsub("ONE\\+","ONE佳",qx_name)
qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
qx_name<-gsub("选装(包|)","佳",qx_name)
qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)

car_name<-data_input$brand_name
car_series1<-data_input$series_name
forFun<-function(i){
  sub(car_series1[i],"",qx_name[i])
}
qx_name<-unlist(lapply(1:length(car_series1),forFun))


##--------------------停用词清洗--------------------
####################################################CROSS
###-------词语描述归一-----
source(paste0(deep_local,"\\config\\config_fun\\fun_stopWords_che58.R",sep=""),echo=TRUE,encoding="utf-8")
output_data<-fun_stopWords_che58(data_input,qx_name)
#------------------数据保存----------------
qx_che58<-data.frame(X=data_input_se$car_id,output_data)

#清洗多余空格
qx_che58<-trim(qx_che58)
qx_che58$car_model_name<-gsub(" +"," ",qx_che58$car_model_name)
qx_che58<-sapply(qx_che58,as.character)
for (i in 1:dim(qx_che58)[2]) {
  qx_che58[,i][which(is.na(qx_che58[,i]))]<-""
}
qx_che58<-data.frame(qx_che58)
qx_che58$X<-as.integer(as.character(qx_che58$X))
#剔除不包含的车系
df_filter<- gsub('-进口','',unique(qx_che58$car_series1)) %>% as.character()
che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
#########################################################################################################
##################################################第二大章：数据匹配#####################################
source(paste0(deep_local,"\\config\\config_fun\\fun_match.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_iteration.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(deep_local,"\\config\\config_fun\\fun_match_result.R",sep=""),echo=TRUE,encoding="utf-8")
data_input<-qx_che58


#step2 function#
fun_step2<-function(){
  file_dir<-deep_local
  che58_city<- read.csv(paste0(file_dir,"/config/config_file/城市牌照.csv",sep=""),header = T,sep = ",")
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.url location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                     " '' state,n.trans_fee,'' transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                     " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='che58' AND match_des='right') m",
                                                     " INNER JOIN spider_www_58 n ON m.id_data_input=n.id",
                                                     " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
  config_distr<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
  config_distr_all<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
  config_series_bcountry<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT yck_brandid,car_country FROM config_vdatabase_yck_brand"),-1)
  dbDisconnect(loc_channel)
  ######
  input_orig$add_time<-as.Date(input_orig$add_time)
  input_orig$mile<-round(input_orig$mile/10000,2)
  input_orig$quotes<-round(input_orig$quotes/10000,2)
  #######location清洗&nbsp
  che58_city$che58_name<-as.character(che58_city$che58_name)
  input_orig$location<-gsub("http://qh.58.*","琼海",input_orig$location)
  input_orig$location<-gsub("http://|.58.com.*","",input_orig$location)
  input_orig<-inner_join(input_orig,che58_city,c("location"="che58_name"))
  input_orig$location<-input_orig$city
  ##########
  input_orig<-inner_join(input_orig,config_series_bcountry,by='yck_brandid')
  user_years<-round((as.Date(input_orig$add_time)-as.Date(input_orig$regDate))/365,2)
  input_orig<-data.frame(input_orig,user_years)
  if(nrow(input_orig)==0){
    print("无数据")
  }else{
    input_orig$regDate<-cut(as.Date(input_orig$regDate),breaks="month")
  }
  input_orig$user_years<-as.numeric(as.character(input_orig$user_years))
  ##年检
  input_orig$annual[grep("年[1-9]月",input_orig$annual)]<-gsub("年","年0",input_orig$annual[grep("年[1-9]月",input_orig$annual)])
  input_orig$annual<-gsub("车主未填写|年","",input_orig$annual)
  input_orig$annual<-gsub("月","01",input_orig$annual)
  input_orig$annual<-gsub("过保","19800101",input_orig$annual)
  input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual,format = "%Y%m%d")
  input_orig$annual[which(input_orig$annual>0)]<-1
  input_orig$annual[which(input_orig$annual<=0)]<-0
  input_orig$annual<-factor(input_orig$annual)
  ##保险
  ##年检
  input_orig$insure[grep("年[1-9]月",input_orig$insure)]<-gsub("年","年0",input_orig$insure[grep("年[1-9]月",input_orig$insure)])
  input_orig$insure<-gsub("车主未填写|年","",input_orig$insure)
  input_orig$insure<-gsub("月","01",input_orig$insure)
  input_orig$insure<-gsub("过保","19800101",input_orig$insure)
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure,format = "%Y%m%d")
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-factor(input_orig$insure)
  ##分区字段(放最后)
  input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-city)
  wutb<-input_orig %>% dplyr::select(-regional)
  
  ###清洗为NA
  wutb$annual[which(is.na(wutb$annual))]<-''
  wutb$transfer[which(is.na(wutb$transfer))]<-''
  wutb$insure[which(is.na(wutb$insure))]<-''
  wutb$state[which(is.na(wutb$state))]<-''
  wutb$trans_fee[which(is.na(wutb$trans_fee))]<-''
  wutb$transfer<-gsub('.*数据|NA','',wutb$transfer)
  wutb$annual<-gsub('NA','',wutb$annual)
  wutb$insure<-gsub('NA','',wutb$insure)
  wutb$state<-gsub('NA','',wutb$state)
  wutb$trans_fee<-gsub('NA','',wutb$trans_fee)
  #清洗color
  wutb$color<-gsub("――|-|无数据|null|[0-9]","",wutb$color)
  wutb$color<-gsub("其他","其它",wutb$color)
  wutb$color<-gsub("色","",wutb$color)
  wutb$color<-gsub("浅|深|象牙|冰川","",wutb$color)
  wutb<-data.frame(wutb,date_add=format(Sys.time(),'%Y-%m-%d'))
  write.csv(wutb,paste0(file_dir,"/file/output/analysis_wide_table.csv",sep=""),
            row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",deep_local,"/file/output/analysis_wide_table.csv'",
                                 " INTO TABLE analysis_wide_table CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='che58'")
  dbDisconnect(loc_channel)
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",deep_local,"/file/output/analysis_wide_table.csv'",
                                 " INTO TABLE analysis_wide_table CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
  dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='che58'")
  dbDisconnect(loc_channel)
}

#step3数据处理
list_result<-tryCatch({fun_match_result(che300,data_input)},
                      error=function(e){0},
                      finally={-1})
if(length(list_result)>1){
  return_db<-list_result$return_db
  return_db<-rbind(wutb_right,return_db)
  match_right<-list_result$match_right
  match_repeat<-list_result$match_repeat
  match_not<-list_result$match_not
  return_db<-data.frame(car_platform="che58",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
  return_db$id_che300<-as.integer(as.character(return_db$id_che300))
  return_db$id_che300[is.na(return_db$id_che300)]<-''
  #判别是否存在正确的匹配
  if(nrow(match_right)>0){
    write.csv(return_db,paste0(deep_local,"\\file\\output\\che58.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
    rizhi<-data.frame(platform=unique(return_db$car_platform),
                      accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                      n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                      n_not=nrow(match_not),add_date=Sys.Date())
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",deep_local,"/file/output/che58.csv'",
                                   " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
    dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",deep_local,"/file/output/che58.csv'",
                                   " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
    dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='che58'"))
    dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
    dbDisconnect(loc_channel)
    loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",deep_local,"/file/output/che58.csv'",
                                   " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
    dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",deep_local,"/file/output/che58.csv'",
                                   " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
    dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='che58'"))
    dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
    dbDisconnect(loc_channel)
    fun_step2()
  }
}