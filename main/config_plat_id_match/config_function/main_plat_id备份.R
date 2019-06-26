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
source(paste0(local_file,"\\config\\config_fun\\fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"\\config\\config_fun\\yck_base_function.R"),echo=FALSE,encoding="utf-8")
local_defin_yun<-fun_mysql_config_up()$local_defin_yun
rm_rule<<- read.csv(paste0(local_file,"\\config\\config_file\\reg_rule.csv"),header = T,sep = ",")
out_rrc<<- read.csv(paste0(local_file,"\\config\\config_file\\out_rrc.csv"),header = T,sep = ",")
source(paste0(local_file,"\\config\\config_fun\\fun_stopWords.R"),echo=TRUE,encoding="utf-8")
source(paste0(local_file,"\\config\\config_fun\\fun_normalization.R"),echo=TRUE,encoding="utf-8")
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
rm_series_rule<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
id_z<<-che300%>%dplyr::select(id_che300=car_id)
dbDisconnect(loc_channel)

fun_plat_id_autohome<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id car_id,brand_name,series_name,model_name model_name_t,model_price FROM config_autohome_major_info_tmp a;"),-1)
  id_autohome<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300,id_autohome,match_type_ah,is_only_ah FROM config_plat_id_match WHERE match_type_ah NOT IN(0,4);"),-1)
  dbDisconnect(loc_channel)
  id<-data.frame(car_id=setdiff(yck_czb$car_id,id_autohome$id_autohome))
  yck_czb<-inner_join(yck_czb,id,by="car_id")
  
  series_all<-yck_czb$series_name
  forFun<-function(i){
    sub(yck_czb$brand_name[i],"",yck_czb$series_name[i])
  }
  series_all<-unlist(lapply(1:length(yck_czb$series_name),forFun))
  linshi<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",yck_czb$model_name_t)
  yck_czb<-data.frame(yck_czb,model_name=paste0(yck_czb$brand_name,series_all,linshi,sep=""))
  yck_czb<-data.frame(yck_czb,car_auto="",discharge_standard="",liter="")
  id<-data.frame(car_id=setdiff(che300$car_id,id_autohome$id_che300))
  che300<-inner_join(che300,id,by="car_id")
  
  
  ##-第一部分：得到车的品牌及车系-##
  #临时车名
  input_test1<-yck_czb
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
  input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  
  #input_test2<-input_test1    input_test1<-input_test2
  #--前期准备：提取准确的brand和series--
  brand_name<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
  brand_name[which(is.na(brand_name))]<-""
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-""
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
  linshi[which(is.na(linshi))]<-""
  series_name<-paste(series_name,linshi,sep = "")
  #-重新将新的brand及series替换-#
  input_test1$brand_name<-brand_name
  input_test1$series_name<-series_name
  input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
  input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
  input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
  
  
  #--第一步：分离--#
  car_name_info<-str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
  a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")
  #全称匹配到车300
  a1$qx_series_all<-as.character(a1$qx_series_all)
  a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第二步：对剩余部分进行全称及系列名匹配--
  seriesFun<-function(i){
    sub(input_test1$brand_name[i],"",input_test1$series_name[i])
  }
  qx_series_des<-unlist(lapply(1:length(input_test1$series_name),seriesFun))
  qx_series_des<-trim(qx_series_des)
  qx_series_all<-paste(input_test1$brand_name,qx_series_des,sep = "")
  qx_series_des[which(nchar(qx_series_des)==0)]<-input_test1$brand_name[which(nchar(qx_series_des)==0)]
  input_test1<-data.frame(input_test1,qx_series_all)
  ##-匹配全称-#
  input_test1$qx_series_all<-as.character(input_test1$qx_series_all)
  #匹配到全称
  a2<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）--
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第四步：未匹配上a4--#
  a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
  a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
  a4$name<-a4$brand_name
  a4$series<-a4$series_name
  
  a4<-a4[,-4]
  yck_czb$car_id<-as.integer(as.character(yck_czb$car_id))
  #-组合所有car_id-##
  data_input_0<-rbind(a1,a2,a3,a4)
  data_input_0<-inner_join(data_input_0,yck_czb[,c("car_id","brand_name","series_name","model_name_t")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name_t,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  #-准确性accurate
  accurate<-c(nrow(a4),nrow(yck_czb))
  rm(a1,a2,a3,a4,car_name_info,input_test1,qx_series_all,qx_series_des)
  gc()
  
  
  ##-第二部分：清洗model_name-##
  #-数据转换名称-
  data_input<-data_input_0
  data_input$model_price<-as.numeric(data_input$model_price)
  qx_name<-toupper(data_input$model_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  car_model_name<-data_input$model_name
  #-part1：得到年款及指导价--car_model_name[-grep("",car_year)]
  qx_name<-gsub("2012年","2012款",qx_name)
  qx_name<-gsub("-2011 款","2011款 ",qx_name)
  car_year<-str_extract(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  car_year<-gsub("款","",car_year)
  car_year[which(nchar(car_year)==2)]<-paste("20",car_year[which(nchar(car_year)==2)],sep = "")
  loc_year<-str_locate(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  car_price<-data_input$model_price
  qx_name<-str_sub(qx_name,loc_year[,2]+1)
  
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
  
  
  #--停用词清洗--#-词语描述归一-
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #--数据保存--
  qx_czb<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_czb<-trim(qx_czb)
  qx_czb$car_model_name<-gsub(" +"," ",qx_czb$car_model_name)
  qx_czb<-sapply(qx_czb,as.character)
  for (i in 1:dim(qx_czb)[2]) {
    qx_czb[,i][which(is.na(qx_czb[,i]))]<-""
  }
  qx_czb<-data.frame(qx_czb)
  qx_czb$X<-as.integer(as.character(qx_czb$X))
  #
  #第二大章：数据匹配##
  source(paste0(local_file,"\\config\\config_fun\\fun_match.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_iteration.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"/main/config_plat_id_match/config_function/id_match.R"),echo=TRUE,encoding="utf-8")
  data_input<-qx_czb
  #调用函数计算结果列表
  che3001<-che300
  names(che300)<-names(qx_czb)
  data_input<-che300
  che300<-qx_czb
  names(che300)<-names(che3001)
  list_result<-fun_match_result_czb(che300,data_input)
  #新增
  che300$car_id<-as.character(che300$car_id)
  che300$car_year<-as.numeric(as.character(che300$car_year))
  che300$car_price<-as.numeric(as.character(che300$car_price))
  return_db<-list_result$return_db%>%dplyr::filter(match_des=='right')%>%
    dplyr::select(id_che300=id_data_input,id_autohome=id_che300)%>%unique()
  return_db$id_autohome<-as.character(return_db$id_autohome)
  return_db<-dplyr::left_join(return_db,che3001,by=c('id_che300'='car_id')) %>% 
    dplyr::select(id_che300,id_autohome,c_car_year=car_year,c_car_price=car_price)
  return_db<-dplyr::left_join(return_db,che300,by=c('id_autohome'='car_id')) %>% 
    dplyr::select(id_che300,id_autohome,c_car_year,car_year,c_car_price,car_price) %>% 
    dplyr::filter(round(abs(c_car_year+c_car_price-car_year-car_price),1)==0) %>% 
    group_by(id_autohome) %>% dplyr::mutate(match_type_ah=ifelse(n()==1,1,2),is_only_ah=ifelse(min(id_che300)-id_che300==0,1,0)) %>% 
    as.data.frame() %>% dplyr::select(id_che300,id_autohome,match_type_ah,is_only_ah)

  #返回关联
  id_autohome$id_che300<-as.character(id_autohome$id_che300)
  id_z$id_che300<-as.character(id_z$id_che300)
  return_db$id_che300<-as.character(return_db$id_che300)
  id_z<-left_join(id_z,id_autohome,by="id_che300")
  result1<-id_z%>%filter(is.na(id_autohome)==FALSE)
  result2<-id_z%>%filter(is.na(id_autohome)==T)%>%dplyr::select(id_che300)
  result_output<-rbind(result1,left_join(result2,return_db,by="id_che300"))
  write.csv(result_output,paste0(local_file,"/main/config_plat_id_match/out_autohome.csv"),row.names = F)
  return(1)
}
fun_plat_id_czb<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id car_id,brand brand_name,series_name,model_name model_name_t,model_price FROM config_chezhibao_major_info a;"),-1)
  id_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300,id_czb FROM config_plat_id_match WHERE id_czb!=0;"),-1)
  dbDisconnect(loc_channel)
  id<-data.frame(car_id=setdiff(yck_czb$car_id,id_czb$id_czb))
  yck_czb<-inner_join(yck_czb,id,by="car_id")
  
  #
  series_all<-yck_czb$series_name
  forFun<-function(i){
    sub(yck_czb$brand_name[i],"",yck_czb$series_name[i])
  }
  series_all<-unlist(lapply(1:length(yck_czb$series_name),forFun))
  linshi<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",yck_czb$model_name_t)
  yck_czb<-data.frame(yck_czb,model_name=paste0(yck_czb$brand_name,series_all,linshi,sep=""))
  ##
  rm_rule<- read.csv(paste0(local_file,"\\config\\config_file\\reg_rule.csv"),header = T,sep = ",")
  out_rrc<- read.csv(paste0(local_file,"\\config\\config_file\\out_rrc.csv"),header = T,sep = ",")
  source(paste0(local_file,"\\config\\config_fun\\fun_stopWords.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_normalization.R"),echo=TRUE,encoding="utf-8")
  yck_czb<-data.frame(yck_czb,car_auto="",discharge_standard="",liter="")
  
  
  ##-第一部分：得到车的品牌及车系-##
  #临时车名
  input_test1<-yck_czb
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
  input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  
  #input_test2<-input_test1    input_test1<-input_test2
  #--前期准备：提取准确的brand和series--
  brand_name<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
  brand_name[which(is.na(brand_name))]<-""
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-""
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
  linshi[which(is.na(linshi))]<-""
  series_name<-paste(series_name,linshi,sep = "")
  #-重新将新的brand及series替换-#
  input_test1$brand_name<-brand_name
  input_test1$series_name<-series_name
  input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
  input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
  input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
  
  
  #--第一步：分离--#
  car_name_info<-str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
  a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")
  #全称匹配到车300
  a1$qx_series_all<-as.character(a1$qx_series_all)
  a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第二步：对剩余部分进行全称及系列名匹配--
  seriesFun<-function(i){
    sub(input_test1$brand_name[i],"",input_test1$series_name[i])
  }
  qx_series_des<-unlist(lapply(1:length(input_test1$series_name),seriesFun))
  qx_series_des<-trim(qx_series_des)
  qx_series_all<-paste(input_test1$brand_name,qx_series_des,sep = "")
  qx_series_des[which(nchar(qx_series_des)==0)]<-input_test1$brand_name[which(nchar(qx_series_des)==0)]
  input_test1<-data.frame(input_test1,qx_series_all)
  ##-匹配全称-#
  input_test1$qx_series_all<-as.character(input_test1$qx_series_all)
  #匹配到全称
  a2<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）--
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第四步：未匹配上a4--#
  a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
  a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
  a4$name<-a4$brand_name
  a4$series<-a4$series_name
  
  a4<-a4[,-4]
  yck_czb$car_id<-as.integer(as.character(yck_czb$car_id))
  #-组合所有car_id-##
  data_input_0<-rbind(a1,a2,a3,a4)
  data_input_0<-inner_join(data_input_0,yck_czb[,c("car_id","brand_name","series_name","model_name_t")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name_t,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  #-准确性accurate
  accurate<-c(nrow(a4),nrow(yck_czb))
  rm(a1,a2,a3,a4,car_name_info,input_test1,qx_series_all,qx_series_des)
  gc()
  
  
  ##-第二部分：清洗model_name-##
  #-数据转换名称-
  data_input<-data_input_0
  qx_name<-toupper(data_input$model_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  car_model_name<-data_input$model_name
  #-part1：得到年款及指导价--car_model_name[-grep("",car_year)]
  qx_name<-gsub("2012年","2012款",qx_name)
  qx_name<-gsub("-2011 款","2011款 ",qx_name)
  car_year<-str_extract(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  car_year<-gsub("款","",car_year)
  car_year[which(nchar(car_year)==2)]<-paste("20",car_year[which(nchar(car_year)==2)],sep = "")
  loc_year<-str_locate(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  car_price<-gsub("万","",data_input$model_price)
  qx_name<-str_sub(qx_name,loc_year[,2]+1)
  
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
  
  
  #--停用词清洗--#-词语描述归一-
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #--数据保存--
  qx_czb<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_czb<-trim(qx_czb)
  qx_czb$car_model_name<-gsub(" +"," ",qx_czb$car_model_name)
  qx_czb<-sapply(qx_czb,as.character)
  for (i in 1:dim(qx_czb)[2]) {
    qx_czb[,i][which(is.na(qx_czb[,i]))]<-""
  }
  qx_czb<-data.frame(qx_czb)
  qx_czb$X<-as.integer(as.character(qx_czb$X))
  #
  #第二大章：数据匹配##
  source(paste0(local_file,"\\config\\config_fun\\fun_match.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_iteration.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"/main/config_plat_id_match/config_function/id_match.R"),echo=TRUE,encoding="utf-8")
  data_input<-qx_czb
  #调用函数计算结果列表
  che3001<-che300
  names(che300)<-names(qx_czb)
  data_input<-che300
  che300<-qx_czb
  names(che300)<-names(che3001)
  list_result<-fun_match_result_czb(che300,data_input)
  return_db<-list_result$return_db%>%filter(match_des=='right')%>%
    dplyr::select(id_che300=id_data_input,id_czb=id_che300)%>%unique()
  
  #返回关联
  id_czb$id_che300<-as.character(id_czb$id_che300)
  id_z$id_che300<-as.character(id_z$id_che300)
  return_db$id_che300<-as.character(return_db$id_che300)
  id_z<-left_join(id_z,id_czb,by="id_che300")
  result1<-id_z%>%filter(is.na(id_czb)==FALSE)
  result2<-id_z%>%filter(is.na(id_czb)==T)%>%dplyr::select(id_che300)
  result_output<-rbind(result1,left_join(result2,return_db,by="id_che300"))
  write.csv(result_output,paste0(local_file,"/main/config_plat_id_match/out_czb.csv"),row.names = F)
  return(1)
}
fun_plat_id_sohu<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT model_year,model_id car_id,brand_name,series_name,model_name model_name_t,model_price,auto car_auto,displacement liter FROM config_souhu_major_info a;"),-1)
  id_souhu<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300,id_souhu FROM config_plat_id_match WHERE id_souhu!=0;"),-1)
  dbDisconnect(loc_channel)
  id<-data.frame(car_id=setdiff(yck_czb$car_id,id_souhu$id_souhu))
  yck_czb<-inner_join(yck_czb,id,by="car_id")
  
  series_all<-yck_czb$series_name
  forFun<-function(i){
    sub(yck_czb$brand_name[i],"",yck_czb$series_name[i])
  }
  series_all<-unlist(lapply(1:length(yck_czb$series_name),forFun))
  linshi<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",yck_czb$model_name_t)
  yck_czb<-data.frame(yck_czb,model_name=paste0(yck_czb$brand_name,series_all,linshi,sep=""))
  ##
  rm_rule<- read.csv(paste0(local_file,"\\config\\config_file\\reg_rule.csv"),header = T,sep = ",")
  out_rrc<- read.csv(paste0(local_file,"\\config\\config_file\\out_rrc.csv"),header = T,sep = ",")
  source(paste0(local_file,"\\config\\config_fun\\fun_stopWords.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_normalization.R"),echo=TRUE,encoding="utf-8")
  yck_czb<-data.frame(yck_czb,discharge_standard="")
  id<-data.frame(car_id=setdiff(che300$car_id,id_souhu$id_che300))
  che300<-inner_join(che300,id,by="car_id")
  
  
  ##-第一部分：得到车的品牌及车系-##
  #临时车名
  input_test1<-yck_czb
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
  input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  
  #input_test2<-input_test1    input_test1<-input_test2
  #--前期准备：提取准确的brand和series--
  brand_name<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
  brand_name[which(is.na(brand_name))]<-""
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-""
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
  linshi[which(is.na(linshi))]<-""
  series_name<-paste(series_name,linshi,sep = "")
  #-重新将新的brand及series替换-#
  input_test1$brand_name<-brand_name
  input_test1$series_name<-series_name
  input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
  input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
  input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
  
  
  #--第一步：分离--#
  car_name_info<-str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
  a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")
  #全称匹配到车300
  a1$qx_series_all<-as.character(a1$qx_series_all)
  a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第二步：对剩余部分进行全称及系列名匹配--
  seriesFun<-function(i){
    sub(input_test1$brand_name[i],"",input_test1$series_name[i])
  }
  qx_series_des<-unlist(lapply(1:length(input_test1$series_name),seriesFun))
  qx_series_des<-trim(qx_series_des)
  qx_series_all<-paste(input_test1$brand_name,qx_series_des,sep = "")
  qx_series_des[which(nchar(qx_series_des)==0)]<-input_test1$brand_name[which(nchar(qx_series_des)==0)]
  input_test1<-data.frame(input_test1,qx_series_all)
  ##-匹配全称-#
  input_test1$qx_series_all<-as.character(input_test1$qx_series_all)
  #匹配到全称
  a2<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）--
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第四步：未匹配上a4--#
  a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
  a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
  a4$name<-a4$brand_name
  a4$series<-a4$series_name
  
  a4<-a4[,c(-2,-5)]
  yck_czb$car_id<-as.integer(as.character(yck_czb$car_id))
  #-组合所有car_id-##
  data_input_0<-rbind(a1,a2,a3,a4)
  data_input_0<-inner_join(data_input_0,yck_czb[,c("car_id","model_year","brand_name","series_name","model_name_t")],by="car_id")%>%
    dplyr::select(car_id,model_year,brand_name=name,series_name=series,model_name=model_name_t,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  #-准确性accurate
  accurate<-c(nrow(a4),nrow(yck_czb))
  rm(a1,a2,a3,a4,car_name_info,input_test1,qx_series_all,qx_series_des)
  gc()
  
  
  ##-第二部分：清洗model_name-##
  #-数据转换名称-
  data_input<-data_input_0
  qx_name<-toupper(data_input$model_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  car_model_name<-data_input$model_name
  #-part1：得到年款及指导价--car_model_name[-grep("",car_year)]
  qx_name<-gsub("2012年","2012款",qx_name)
  qx_name<-gsub("-2011 款","2011款 ",qx_name)
  car_year<-data_input$model_year
  car_year<-gsub(" |款","",car_year)
  car_price<-data_input$model_price
  
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
  
  
  #--停用词清洗--#-词语描述归一-
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #--数据保存--
  qx_czb<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_czb<-trim(qx_czb)
  qx_czb$car_model_name<-gsub(" +"," ",qx_czb$car_model_name)
  qx_czb<-sapply(qx_czb,as.character)
  for (i in 1:dim(qx_czb)[2]) {
    qx_czb[,i][which(is.na(qx_czb[,i]))]<-""
  }
  qx_czb<-data.frame(qx_czb)
  qx_czb$X<-as.integer(as.character(qx_czb$X))
  #
  #第二大章：数据匹配##
  source(paste0(local_file,"\\config\\config_fun\\fun_match.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_iteration.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"/main/config_plat_id_match/config_function/id_match.R"),echo=TRUE,encoding="utf-8")
  data_input<-qx_czb
  #调用函数计算结果列表
  che3001<-che300
  names(che300)<-names(qx_czb)
  data_input<-che300
  che300<-qx_czb
  names(che300)<-names(che3001)
  list_result<-fun_match_result_czb(che300,data_input)
  return_db<-list_result$return_db%>%filter(match_des=='right')%>%
    dplyr::select(id_che300=id_data_input,id_souhu=id_che300)%>%unique()
  
  #返回关联
  id_souhu$id_che300<-as.character(id_souhu$id_che300)
  id_z$id_che300<-as.character(id_z$id_che300)
  return_db$id_che300<-as.character(return_db$id_che300)
  id_z<-left_join(id_z,id_souhu,by="id_che300")
  result1<-id_z%>%filter(is.na(id_souhu)==FALSE)
  result2<-id_z%>%filter(is.na(id_souhu)==T)%>%dplyr::select(id_che300)
  result_output<-rbind(result1,left_join(result2,return_db,by="id_che300"))
  write.csv(result_output,paste0(local_file,"/main/config_plat_id_match/out_souhu.csv"),row.names = F)
  return(1)
}
fun_plat_id_yiche<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_czb<-dbFetch(dbSendQuery(loc_channel,"SELECT model_year,model_id car_id,brand_name,series_name,model_name model_name_t,model_price FROM config_yiche_major_info a;"),-1)
  id_yiche<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300,id_yiche FROM config_plat_id_match WHERE id_yiche!=0;"),-1)
  dbDisconnect(loc_channel)
  id<-data.frame(car_id=setdiff(yck_czb$car_id,id_yiche$id_yiche))
  yck_czb<-inner_join(yck_czb,id,by="car_id")
  
  series_all<-yck_czb$series_name
  forFun<-function(i){
    sub(yck_czb$brand_name[i],"",yck_czb$series_name[i])
  }
  series_all<-unlist(lapply(1:length(yck_czb$series_name),forFun))
  linshi<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",yck_czb$model_name_t)
  yck_czb<-data.frame(yck_czb,model_name=paste0(yck_czb$brand_name,series_all,linshi,sep=""))
  ##
  rm_rule<- read.csv(paste0(local_file,"\\config\\config_file\\reg_rule.csv"),header = T,sep = ",")
  out_rrc<- read.csv(paste0(local_file,"\\config\\config_file\\out_rrc.csv"),header = T,sep = ",")
  source(paste0(local_file,"\\config\\config_fun\\fun_stopWords.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_normalization.R"),echo=TRUE,encoding="utf-8")
  yck_czb<-data.frame(yck_czb,car_auto="",discharge_standard="",liter="")
  
  
  ##-第一部分：得到车的品牌及车系-##
  #临时车名
  input_test1<-yck_czb
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)]<-sub("赛欧","",input_test1$model_name[grep("赛欧.*赛欧3",input_test1$model_name)])
  input_test1$model_name[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test1$model_name)]<-"别克赛欧"
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  
  #input_test2<-input_test1    input_test1<-input_test2
  #--前期准备：提取准确的brand和series--
  brand_name<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
  brand_name[which(is.na(brand_name))]<-""
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-""
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口")
  linshi[which(is.na(linshi))]<-""
  series_name<-paste(series_name,linshi,sep = "")
  #-重新将新的brand及series替换-#
  input_test1$brand_name<-brand_name
  input_test1$series_name<-series_name
  input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
  input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
  input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
  
  
  #--第一步：分离--#
  car_name_info<-str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
  a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")
  #全称匹配到车300
  a1$qx_series_all<-as.character(a1$qx_series_all)
  a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第二步：对剩余部分进行全称及系列名匹配--
  seriesFun<-function(i){
    sub(input_test1$brand_name[i],"",input_test1$series_name[i])
  }
  qx_series_des<-unlist(lapply(1:length(input_test1$series_name),seriesFun))
  qx_series_des<-trim(qx_series_des)
  qx_series_all<-paste(input_test1$brand_name,qx_series_des,sep = "")
  qx_series_des[which(nchar(qx_series_des)==0)]<-input_test1$brand_name[which(nchar(qx_series_des)==0)]
  input_test1<-data.frame(input_test1,qx_series_all)
  ##-匹配全称-#
  input_test1$qx_series_all<-as.character(input_test1$qx_series_all)
  #匹配到全称
  a2<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）--
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  #--第四步：未匹配上a4--#
  a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
  a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
  a4$name<-a4$brand_name
  a4$series<-a4$series_name
  
  a4<-a4[,c(-2,-5)]
  yck_czb$car_id<-as.integer(as.character(yck_czb$car_id))
  #-组合所有car_id-##
  data_input_0<-rbind(a1,a2,a3,a4)
  data_input_0<-inner_join(data_input_0,yck_czb[,c("car_id","model_year","brand_name","series_name","model_name_t")],by="car_id")%>%
    dplyr::select(car_id,model_year,brand_name=name,series_name=series,model_name=model_name_t,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  #-准确性accurate
  accurate<-c(nrow(a4),nrow(yck_czb))
  rm(a1,a2,a3,a4,car_name_info,input_test1,qx_series_all,qx_series_des)
  gc()
  
  
  ##-第二部分：清洗model_name-##
  #-数据转换名称-
  data_input<-data_input_0
  qx_name<-toupper(data_input$model_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  car_model_name<-data_input$model_name
  #-part1：得到年款及指导价--car_model_name[-grep("",car_year)]
  qx_name<-gsub("2012年","2012款",qx_name)
  qx_name<-gsub("-2011 款","2011款 ",qx_name)
  car_year<-data_input$model_year
  car_year<-gsub(" |款","",car_year)
  car_price<-data_input$model_price
  
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
  
  
  #--停用词清洗--#-词语描述归一-
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #--数据保存--
  qx_czb<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_czb<-trim(qx_czb)
  qx_czb$car_model_name<-gsub(" +"," ",qx_czb$car_model_name)
  qx_czb<-sapply(qx_czb,as.character)
  for (i in 1:dim(qx_czb)[2]) {
    qx_czb[,i][which(is.na(qx_czb[,i]))]<-""
  }
  qx_czb<-data.frame(qx_czb)
  qx_czb$X<-as.integer(as.character(qx_czb$X))
  #
  #第二大章：数据匹配##
  source(paste0(local_file,"\\config\\config_fun\\fun_match.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"\\config\\config_fun\\fun_iteration.R"),echo=TRUE,encoding="utf-8")
  source(paste0(local_file,"/main/config_plat_id_match/config_function/id_match.R"),echo=TRUE,encoding="utf-8")
  data_input<-qx_czb
  #调用函数计算结果列表
  che3001<-che300
  names(che300)<-names(qx_czb)
  data_input<-che300
  che300<-qx_czb
  names(che300)<-names(che3001)
  list_result<-fun_match_result_czb(che300,data_input)
  return_db<-list_result$return_db%>%filter(match_des=='right')%>%
    dplyr::select(id_che300=id_data_input,id_yiche=id_che300)%>%unique()
  
  #返回关联
  id_yiche$id_che300<-as.character(id_yiche$id_che300)
  id_z$id_che300<-as.character(id_z$id_che300)
  return_db$id_che300<-as.character(return_db$id_che300)
  id_z<-left_join(id_z,id_yiche,by="id_che300")
  result1<-id_z%>%filter(is.na(id_yiche)==FALSE)
  result2<-id_z%>%filter(is.na(id_yiche)==T)%>%dplyr::select(id_che300)
  result_output<-rbind(result1,left_join(result2,return_db,by="id_che300"))
  write.csv(result_output,paste0(local_file,"/main/config_plat_id_match/out_yiche.csv"),row.names = F)
  return(1)
}

#执行函数，异常邮件抛出#
if(tryCatch({fun_plat_id_autohome()},error=function(e){0},finally={0})!=1){
  fun_mailsend("车型库匹配异常config_platid",'汽车之家车型库匹配失败')}
if(tryCatch({fun_plat_id_czb()},error=function(e){0},finally={0})!=1){
  fun_mailsend("车型库匹配异常config_platid",'车置宝车型库匹配失败')}
if(tryCatch({fun_plat_id_sohu()},error=function(e){0},finally={0})!=1){
  fun_mailsend("车型库匹配异常config_platid",'搜狐车型库匹配失败')}
if(tryCatch({fun_plat_id_yiche()},error=function(e){0},finally={0})!=1){
  fun_mailsend("车型库匹配异常config_platid",'易车车型库匹配失败')}


loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
id_che300<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)%>%dplyr::select(id_che300=car_id)
dbDisconnect(loc_channel)
id_autohome<-read.csv(paste0(local_file,"/main/config_plat_id_match/out_autohome.csv"))
id_souhu<-read.csv(paste0(local_file,"/main/config_plat_id_match/out_souhu.csv"))
id_yiche<-read.csv(paste0(local_file,"/main/config_plat_id_match/out_yiche.csv"))
id_czb<-read.csv(paste0(local_file,"/main/config_plat_id_match/out_czb.csv"))
id_result<-left_join(left_join(left_join(left_join(id_che300,id_autohome,by="id_che300"),id_souhu,by="id_che300"),id_yiche,by="id_che300"),id_czb,by="id_che300")%>%unique()
for (i in 1:dim(id_result)[2]) {
  id_result[,i][which(is.na(id_result[,i]))]<-0
}
##入库
write.csv(id_result,paste0(local_file,"/main/config_plat_id_match/id_result.csv"),
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/main/config_plat_id_match/id_result.csv'",
                               " REPLACE INTO TABLE config_plat_id_match CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
dbDisconnect(loc_channel)