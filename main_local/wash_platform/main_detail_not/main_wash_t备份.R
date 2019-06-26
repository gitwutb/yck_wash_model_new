rm(list = ls(all=T))
gc()
library(dplyr)
library(stringr)
library(raster)
library(RMySQL)
library(reshape2)
local_file<<-gsub("(\\/bat|\\/main).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
source(paste0(local_file,"/config/config_fun/yck_base_function.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/dealFun_vdatabase.R"),echo=FALSE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/matchFun_vdatabase.R"),echo=TRUE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/fun_mysql_config_up.R"),echo=FALSE,encoding="utf-8")
local_defin<<-fun_mysql_config_up()$local_defin
local_defin_yun<<-fun_mysql_config_up()$local_defin_yun
data_new<-Sys.Date()%>%as.character()
#全局变量
source(paste0(local_file,"/config/config_fun/fun_match_result.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/fun_match_result_czb.R",sep=""),echo=TRUE,encoding="utf-8")
source(paste0(local_file,"/config/config_fun/fun_match_result_yiche.R",sep=""),echo=TRUE,encoding="utf-8")
rm_che58<<- read.csv(paste0(local_file,"/config/config_file/reg_che58.csv",sep=""),header = T,sep = ",")
rm_rule<<- read.csv(paste0(local_file,"/config/config_file/reg_rule.csv"),header = T,sep = ",")
out_rrc<<- read.csv(paste0(local_file,"/config/config_file/out_rrc.csv"),header = T,sep = ",")
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
rm_series_rule<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM config_reg_series_rule;"),-1)
che300<<-dbFetch(dbSendQuery(loc_channel,"SELECT * FROM analysis_che300_cofig_info;"),-1)
config_distr<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_distr_all<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                  INNER JOIN config_district_regional b ON a.key_province=b.province;"),-1)
config_series_bcountry<<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT yck_brandid,car_country FROM config_vdatabase_yck_brand"),-1)
dbDisconnect(loc_channel)


washfun_che168<-function(){
  #步骤一：拿取二手车信息
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_che168<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,quotes model_price,gearbox car_auto FROM spider_www_168 a
                                WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('che168'));"),-1)
  dbDisconnect(loc_channel)
  #------第一部分：得到车的品牌及车系-------#
  ##临时车名
  input_test1<-yck_che168
  input_test1$brand_name<-gsub(" ","",input_test1$brand_name)
  input_test1$series_name<-gsub(" ","",input_test1$series_name)
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
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
  brand_name<-str_extract(input_test1$brand_name,c(str_c(unique(rm_series_rule$rule_name),sep="",collapse = "|")))
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
  data_input_0<-inner_join(data_input_0,yck_che168[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  data_input_0<-data.frame(data_input_0,discharge_standard="",liter="")
  #---准确性accurate
  accurate<-c(length(a4),nrow(yck_che168))
  rm(a1,a2,a3,a4,brand_name,series_name,car_name_info,input_test1,qx_series_all,qx_series_des)
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
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  qx_che168<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_che168<-trim(qx_che168)
  qx_che168$car_model_name<-gsub(" +"," ",qx_che168$car_model_name)
  qx_che168<-sapply(qx_che168,as.character)
  for (i in 1:dim(qx_che168)[2]) {
    qx_che168[,i][which(is.na(qx_che168[,i]))]<-""
  }
  qx_che168<-data.frame(qx_che168)
  qx_che168$X<-as.integer(as.character(qx_che168$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_che168$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##################################################第二大章：数据匹配#####################################
  data_input<-qx_che168
  ##调用函数计算结果列表
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " n.state,n.trans_fee,n.transfer,n.annual,n.insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='che168' AND match_des='right') m",
                                                       " INNER JOIN spider_www_168 n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
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
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险
    input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
    input_orig$insure[which(input_orig$insure>0)]<-1
    input_orig$insure[which(input_orig$insure<=0)]<-0
    input_orig$insure<-factor(input_orig$insure)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="che168",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\che168.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='che168'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/che168.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/che168.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='che168'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
  return(1)
}
washfun_che58<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  yck_che58<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,model model_name,emission discharge_standard,gearbox car_auto FROM spider_www_58 a
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('che58'));"),-1)
  dbDisconnect(loc_channel)
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
  
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
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
  ##第二大章：数据匹配##
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
  
  
  source(paste0(local_file,"\\config\\config_fun\\fun_stopWords_che58.R",sep=""),echo=TRUE,encoding="utf-8")
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords_che58(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
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
  ##################################################第二大章：数据匹配#####################################
  data_input<-qx_che58
  
  
  #step2 function#
  fun_step2<-function(){
    che58_city<- read.csv(paste0(local_file,"/config/config_file/城市牌照.csv",sep=""),header = T,sep = ",")
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.url location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " '' state,n.trans_fee,'' transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='che58' AND match_des='right') m",
                                                       " INNER JOIN spider_www_58 n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
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
      write.csv(return_db,paste0(local_file,"\\file\\output\\che58.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='che58'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/che58.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/che58.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='che58'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
  return(1)
}
washfun_csp<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  yck_csp<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,model model_name,emission discharge_standard,gearbox car_auto FROM spider_www_chesupai a
                             WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('csp'));"),-1)
  dbDisconnect(loc_channel)
  yck_csp<-data.frame(car_id=yck_csp$car_id,brand_name="",series_name="",yck_csp[,-1],model_price="")
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_csp
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
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
  car_name_info<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
  #a1<-data.frame(car_id=input_test1[grep("",car_name_info),"car_id"],qx_series_all=car_name_info[grep("",car_name_info)])
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
  data_input_0<-inner_join(data_input_0,yck_csp[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  data_input_0<-data.frame(data_input_0,liter="")
  #---准确性accurate
  accurate<-c(length(a4),nrow(yck_csp))
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
  
  ###--------清洗奔驰--增加奥迪----
  car_series1<-gsub("GRAND EDITION","特别版",car_series1)
  car_series1<-gsub("改装房车","",car_series1)
  qx_name[grep("奔驰",car_series1)]<-gsub("AMG","",qx_name[grep("奔驰",car_series1)])
  benchi<-str_extract(car_series1,"奥迪TTS|奥迪TT|奔驰.*")
  benchi<-gsub("奥迪|奔驰|(级| |-).*","",benchi)
  benchi[which(is.na(benchi))]<-""
  #清除e L等
  forFun<-function(i){
    sub(benchi[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  ##获取奔驰数字串
  linshi<-str_extract(car_series1,"奔驰.*")
  linshi[grep("奔驰",car_series1)]<-str_extract(qx_name[grep("奔驰",car_series1)],"[0-9]{3}|[0-9]{2}")
  linshi[which(is.na(linshi))]<-""
  benchi<-paste(benchi,linshi,sep = "")
  #清除数字
  forFun<-function(i){
    sub(linshi[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  car_series1[grep("奔驰",car_series1)]<-gsub("级","",car_series1[grep("奔驰",car_series1)])
  
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
  qx_csp<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_csp<-trim(qx_csp)
  qx_csp$car_model_name<-gsub(" +"," ",qx_csp$car_model_name)
  qx_csp<-sapply(qx_csp,as.character)
  for (i in 1:dim(qx_csp)[2]) {
    qx_csp[,i][which(is.na(qx_csp[,i]))]<-""
  }
  qx_csp<-data.frame(qx_csp)
  qx_csp$X<-as.integer(as.character(qx_csp$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_csp$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##################################################第二大章：数据匹配#####################################
  data_input<-qx_csp
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " n.state,'0' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='csp' AND match_des='right') m",
                                                       " INNER JOIN spider_www_chesupai n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    ###location清洗&nbsp
    input_orig$location<-gsub("&nbsp","",input_orig$location)
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    location_ls[which(is.na(location_ls))]<-str_extract(input_orig$address[which(is.na(location_ls))],paste0(config_distr$city,sep="",collapse = "|"))
    location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
    location_ls1$key_county<-as.character(location_ls1$key_county)
    location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
    input_orig$location<-location_ls
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
    #
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
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险
    input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
    input_orig$insure[which(input_orig$insure>0)]<-1
    input_orig$insure[which(input_orig$insure<=0)]<-0
    input_orig$insure<-factor(input_orig$insure)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-address)
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="csp",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\csp.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='csp'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/csp.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/csp.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='csp'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
  return(1)
}
washfun_czb<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  # field.name<-dbListFields(loc_channel,"")
  yck_czp<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,emission discharge_standard,displacement liter FROM spider_www_chezhibao a
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('czb'));"),-1)
  dbDisconnect(loc_channel)
  yck_czp<-data.frame(yck_czp,model_price="",car_auto="")
  yck_czp$discharge_standard<-gsub("2|Ⅱ|II","二",yck_czp$discharge_standard)
  yck_czp$discharge_standard<-gsub("3|III|Ⅲ","三",yck_czp$discharge_standard)
  yck_czp$discharge_standard<-gsub("4|IV|Ⅳ","四",yck_czp$discharge_standard)
  yck_czp$discharge_standard<-gsub("5|V","五",yck_czp$discharge_standard)
  yck_czp$discharge_standard<-gsub("6","六",yck_czp$discharge_standard)
  
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_czp
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
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
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
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
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  ###----------------第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）-----------
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
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
  data_input_0<-inner_join(data_input_0,yck_czp[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  #---准确性accurate
  accurate<-c(length(a4),nrow(yck_czp))
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
  
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
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
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_czb$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##第二大章：数据匹配##
  data_input<-qx_czb
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.bid_price quotes,p.model_price,n.mile,",
                                                       " '' state,'' trans_fee,n.transfer,n.annual,'' insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='czb' AND match_des='right') m",
                                                       " INNER JOIN spider_www_chezhibao n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    
    #######location清洗&nbsp
    input_orig$location<-gsub("襄樊","襄阳",input_orig$location)
    input_orig$location<-gsub("杨凌|杨陵","咸阳",input_orig$location)
    input_orig$location<-gsub("农垦系统","哈尔滨",input_orig$location)
    input_orig$location<-gsub("湖北","武汉",input_orig$location)
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    #通过区县
    location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
    location_ls1$key_county<-as.character(location_ls1$key_county)
    location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
    #最终
    input_orig$location<-location_ls
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
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
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险(无)
    ##转手(大于等于1次)
    input_orig$transfer<-gsub("无","0",input_orig$transfer)
    input_orig$transfer<-gsub(".*-.*","1",input_orig$transfer)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result_czb(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="czb",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\czb.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='czb'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/czb.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/czb.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='czb'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
}
washfun_guazi<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  # field.name<-dbListFields(loc_channel,"")
  yck_guazi<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,model model_name,emission discharge_standard,gearbox car_auto FROM spider_www_guazi a
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('guazi'));"),-1)
  dbDisconnect(loc_channel)
  yck_guazi<-data.frame(car_id=yck_guazi$car_id,brand_name="",series_name="",yck_guazi[,-1],model_price="")
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_guazi
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
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
  data_input_0<-inner_join(data_input_0,yck_guazi[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  data_input_0<-data.frame(data_input_0,liter="")
  #---准确性accurate
  accurate<-c(length(a4),nrow(yck_guazi))
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
  
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
  qx_guazi<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_guazi<-trim(qx_guazi)
  qx_guazi$car_model_name<-gsub(" +"," ",qx_guazi$car_model_name)
  qx_guazi<-sapply(qx_guazi,as.character)
  for (i in 1:dim(qx_guazi)[2]) {
    qx_guazi[,i][which(is.na(qx_guazi[,i]))]<-""
  }
  qx_guazi<-data.frame(qx_guazi)
  qx_guazi$X<-as.integer(as.character(qx_guazi$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_guazi$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##第二大章：数据匹配##
  data_input<-qx_guazi
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'-' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " n.state,'0' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='guazi' AND match_des='right') m",
                                                       " INNER JOIN spider_www_guazi n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    #######location清洗&nbsp
    input_orig$location<-gsub("襄樊","襄阳",input_orig$location)
    input_orig$location<-gsub("杨凌","咸阳",input_orig$location)
    input_orig$location<-gsub("农垦系统","哈尔滨",input_orig$location)
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    location_ls[which(is.na(location_ls))]<-str_extract(input_orig$address[which(is.na(location_ls))],paste0(config_distr$city,sep="",collapse = "|"))
    location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
    location_ls1$key_county<-as.character(location_ls1$key_county)
    location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
    #剩余各区存在重名
    location_ls2<-data.frame(key_county=input_orig$address[which(is.na(location_ls))])
    location_ls2$key_county<-as.character(location_ls2$key_county)
    laji<-inner_join(location_ls2,config_distr_all,by="key_county")
    laji1<-as.data.frame(table(inner_join(location_ls2,config_distr_all,by="key_county")[,c("city","key_county")]%>%unique()%>%.[2]))%>%filter(Freq==1)
    ##########2018/05/07修改#############
    if(nrow(laji1)>0){
      laji<-inner_join(laji,laji1,c("key_county"="Var1"))[,c("city","key_county")]%>%unique()
      location_ls[which(is.na(location_ls))]<-as.character(left_join(location_ls2,laji,by="key_county")[,"city"])
      location_ls[which(is.na(location_ls))]<-""
    }else{
      location_ls[which(is.na(location_ls))]<-""
    }
    
    #最终
    input_orig$location<-location_ls
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
    rm(laji,laji1,location_ls2)
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
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险
    input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
    input_orig$insure[which(input_orig$insure>0)]<-1
    input_orig$insure[which(input_orig$insure<=0)]<-0
    input_orig$insure<-factor(input_orig$insure)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-address)
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="guazi",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\guazi.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='guazi'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/guazi.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/guazi.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='guazi'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
  return(1)
}
washfun_rrc<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  # field.name<-dbListFields(loc_channel,"")
  yck_rrc<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,tax_price model_price,emission discharge_standard,displacement liter FROM spider_www_renren a 
                             WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('rrc'));"),-1)
  dbDisconnect(loc_channel)
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_rrc
  input_test1$brand_name<-gsub(" ","",input_test1$brand_name)
  input_test1$series_name<-gsub(" ","",input_test1$series_name)
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$brand_name<-fun_normalization(input_test1$brand_name)
  input_test1$series_name<-fun_normalization(input_test1$series_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$series_name<-gsub("MINI |SMART |H-1|VELOSTER|劳恩斯-|别克|\\·","",input_test1$series_name)
  input_test1$series_name[grep("劲炫ASX",input_test1$model_name)]<-gsub("劲炫ASX|劲炫","劲炫ASX",input_test1$series_name[grep("劲炫ASX",input_test1$model_name)])
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  brand_name<-str_extract(input_test1$brand_name,c(str_c(unique(rm_series_rule$rule_name),sep="",collapse = "|")))
  brand_name[which(is.na(brand_name))]<-input_test1$brand_name[which(is.na(brand_name))]
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  series_name<-str_extract(input_test1$series_name,gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-input_test1$series_name[which(is.na(series_name))]
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|N3")
  linshi[which(is.na(linshi))]<-""
  linshi[-grep("夏利",series_name)]<-""
  series_name<-paste(series_name,linshi,sep = "")
  series_name[grep("赛欧3",input_test1$model_name)]<-str_extract(input_test1$model_name[grep("赛欧3",input_test1$model_name)],"赛欧3")
  ####------重新将新的brand及series替换------##
  input_test1$brand_name<-brand_name
  input_test1$series_name<-series_name
  input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
  input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
  input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
  input_test1$series_name[grep("COUNTRYMAN",input_test1$model_name)]<-"MINI"
  
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
  a1<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,id)
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")%>%dplyr::select(-qx_series_all)
  ####################################第二轮：系列名匹配######################
  a2<-inner_join(input_test1,rm_series_rule,c("series_name"="series"))%>%
    dplyr::select(car_id,id)
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")
  data_input_0<-rbind(a1,a2)
  data_input_0<-inner_join(data_input_0,yck_rrc,by="car_id")
  data_input_0<-inner_join(data_input_0,rm_series_rule,by="id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name,model_price,discharge_standard,liter)
  data_input_0<-rbind(data_input_0,input_test1)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0<-data.frame(data_input_0,auto="")
  #---准确性accurate
  accurate<-c(nrow(a3),nrow(yck_rrc))
  rm(a1,a2,a3,input_test1,qx_series_all,qx_series_des,linshi,linshi_series,brand_name)
  gc()
  
  
  ######################------第二部分：清洗model_name-------#################
  #-----数据转换名称------
  data_input<-data_input_0
  qx_name<-toupper(data_input$model_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  car_model_name<-data_input$model_name
  
  ##-------------part1：得到年款及指导价----------------#
  qx_name<-gsub("2012年","2012款",qx_name)
  qx_name<-gsub("-2011 款","2011款 ",qx_name)
  car_year<-str_extract(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
  car_year<-gsub("款","",car_year)
  loc_year<-str_locate(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
  car_price<-round(data_input$model_price/10000,2)
  qx_name<-str_sub(qx_name,loc_year[,2]+1)
  #清除年年款
  qx_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",qx_name)
  qx_name<-gsub("\\（","(",qx_name)
  qx_name<-gsub("\\）",")",qx_name)
  qx_name<-gsub("\\(进口\\)","",qx_name)
  qx_name<-gsub("\\(海外\\)|赛欧3","",qx_name)
  qx_name<-gsub("POWER DAILY","宝迪",qx_name)
  qx_name<-gsub("III","Ⅲ",qx_name)
  qx_name<-gsub("II","Ⅱ",qx_name)
  qx_name<-gsub("—|－","-",qx_name)
  qx_name<-gsub("\\·|\\?|(-|)JL465Q","",qx_name)
  qx_name<-gsub("ONE\\+","ONE佳",qx_name)
  qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
  qx_name<-gsub("选装(包|)","佳",qx_name)
  qx_name<-gsub("格锐","格越",qx_name)
  qx_name<-gsub("Axela昂克赛拉|Axela","昂克赛拉",qx_name)
  qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)
  
  ##car_name---------得到品牌名-系列名---------qx<-data.frame(qx_name)
  car_name<-toupper(data_input$brand_name)
  car_series1<-toupper(data_input$series_name)
  
  ##2、清洗系列
  seriesFun<-function(i){
    gsub(car_series1[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_name),seriesFun))
  
  
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
  qx_rrc<-data.frame(X=data_input_0$car_id,output_data)
  #清洗多余空格
  qx_rrc<-trim(qx_rrc)
  qx_rrc$car_model_name<-gsub(" +"," ",qx_rrc$car_model_name)
  qx_rrc<-sapply(qx_rrc,as.character)
  for (i in 1:dim(qx_rrc)[2]) {
    qx_rrc[,i][which(is.na(qx_rrc[,i]))]<-""
  }
  qx_rrc<-data.frame(qx_rrc)
  qx_rrc$X<-as.integer(as.character(qx_rrc$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_rrc$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##第二大章：数据匹配##
  data_input<-qx_rrc
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " n.state,'1' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='rrc' AND match_des='right') m",
                                                       " INNER JOIN spider_www_renren n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    ###location清洗
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    location_ls[which(is.na(location_ls))]<-str_extract(input_orig$address[which(is.na(location_ls))],paste0(config_distr$city,sep="",collapse = "|"))
    input_orig$location<-location_ls
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
    #
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
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险
    input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
    input_orig$insure[which(input_orig$insure>0)]<-1
    input_orig$insure[which(input_orig$insure<=0)]<-0
    input_orig$insure<-factor(input_orig$insure)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))%>%dplyr::select(-address)
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="rrc",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\rrc.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='rrc'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/rrc.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/rrc.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='rrc'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
  
  ###输出已经匹配的人人车配置表
  out_rrc<-read.csv(paste0(local_file,"\\config\\config_file\\out_rrc.csv",sep=""),header = T,sep = ",")
  out_rrc_append<-unique(inner_join(match_right,qx_rrc,c("id_data_input"="X"))%>%dplyr::select(id_che300,cname=car_model_name,car_year,car_name,car_series1,car_price))
  out_rrc<-unique(rbind(out_rrc,out_rrc_append))
  write.csv(out_rrc,paste0(local_file,"\\config\\config_file\\out_rrc.csv",sep=""),row.names = F)
}
washfun_souche<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  # field.name<-dbListFields(loc_channel,"")
  yck_czp<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,emission discharge_standard,displacement liter FROM spider_www_souche a
                             WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('souche'));"),-1)
  dbDisconnect(loc_channel)
  yck_czp<-data.frame(yck_czp,model_price="",car_auto="")
  
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_czp
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("昂科赛拉","昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
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
  car_name_info<-str_extract(paste(input_test1$model_name,sep = ""),c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
  a1<-data.frame(input_test1[grep("",car_name_info),],qx_series_all=car_name_info[grep("",car_name_info)])
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")
  #全称匹配到车300
  a1$qx_series_all<-as.character(a1$qx_series_all)
  a1<-inner_join(a1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
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
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
  ###----------------第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）-----------
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,model_price,id,name,series,qx_series_all,discharge_standard,car_auto,liter)
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
  data_input_0<-inner_join(data_input_0,yck_czp[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  #---准确性accurate
  accurate<-c(nrow(a4),nrow(yck_czp))
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
  car_series1<-gsub("-进口","",data_input$series_name)
  forFun<-function(i){
    sub(paste0(".*",car_series1[i],sep=""),"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  qx_name<-gsub("奔驰.*级 |奔驰.*级AMG|奔驰","",qx_name)
  car_series1<-data_input$series_name
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
  qx_souche<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_souche<-trim(qx_souche)
  qx_souche$car_model_name<-gsub(" +"," ",qx_souche$car_model_name)
  qx_souche<-sapply(qx_souche,as.character)
  for (i in 1:dim(qx_souche)[2]) {
    qx_souche[,i][which(is.na(qx_souche[,i]))]<-""
  }
  qx_souche<-data.frame(qx_souche)
  qx_souche$X<-as.integer(as.character(qx_souche$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_souche$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##第二大章：数据匹配##
  data_input<-qx_souche
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " '' state,'' trans_fee,'' transfer,'' annual,'' insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='souche' AND match_des='right') m",
                                                       " INNER JOIN spider_www_souche n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    
    #######location清洗&nbsp
    input_orig$location<-gsub("襄樊","襄阳",input_orig$location)
    input_orig$location<-gsub("杨凌|杨陵","咸阳",input_orig$location)
    input_orig$location<-gsub("农垦系统","哈尔滨",input_orig$location)
    input_orig$location<-gsub("湖北","武汉",input_orig$location)
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    #通过区县
    location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
    location_ls1$key_county<-as.character(location_ls1$key_county)
    location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
    #最终
    input_orig$location<-location_ls
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
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
    ##年检(无)
    ##保险(无)
    ##转手(大于等于1次)
    input_orig$transfer<-gsub("无","0",input_orig$transfer)
    input_orig$transfer<-gsub(".*-.*","1",input_orig$transfer)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="souche",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\souche.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='souche'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/souche.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/souche.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='souche'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
}
washfun_yiche<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  # field.name<-dbListFields(loc_channel,"")
  yck_yiche<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name,msrp model_price FROM spider_www_yiche a 
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('yiche'));"),-1)
  dbDisconnect(loc_channel)
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_yiche
  input_test1$brand_name<-gsub(" ","",input_test1$brand_name)
  input_test1$series_name<-gsub(" ","",input_test1$series_name)
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  #input_test1清洗
  input_test1$brand_name<-fun_normalization(input_test1$brand_name)
  input_test1$series_name<-fun_normalization(input_test1$series_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  input_test1$model_name<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test1$model_name)
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub("神行者2|神行者","神行者2",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(1990:2030,"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$series_name<-gsub("三厢|两厢","",input_test1$series_name)
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  
  ############0920##############
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  ##补充新的name
  brand_name<-str_extract(input_test1$brand_name,c(str_c(unique(rm_series_rule$rule_name),sep="",collapse = "|")))
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  linshi1<-data.frame(series=str_extract(paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = "")[which(is.na(brand_name))],gsub(" ","",linshi_series)))
  linshi1$series<-as.character(linshi1$series)
  linshi1<-right_join(rm_series_rule,linshi1,by="series")%>%dplyr::select(name)
  brand_name[which(is.na(brand_name))]<-as.character(linshi1$name)
  brand_name[which(is.na(brand_name))]<-input_test1$brand_name[which(is.na(brand_name))]
  ##补充新的series
  series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
  input_test1$model_name<-paste(input_test1$brand_name,input_test1$series_name,input_test1$model_name,sep = "")
  series_name[which(is.na(series_name))]<-str_extract(input_test1$model_name[which(is.na(series_name))],gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-input_test1$series_name[which(is.na(series_name))]
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|N3")
  linshi[which(is.na(linshi))]<-""
  linshi[-grep("夏利",series_name)]<-""
  series_name<-paste(series_name,linshi,sep = "")
  series_name<-gsub("A佳A佳","A佳",series_name)
  series_name<-gsub("N3佳N3佳","N3佳",series_name)
  series_name<-gsub("N3N3","N3",series_name)
  series_name[grep("赛欧3",input_test1$model_name)]<-str_extract(input_test1$model_name[grep("赛欧3",input_test1$model_name)],"赛欧3")
  ####------重新将新的brand及series替换------##
  input_test1$brand_name<-brand_name
  input_test1$series_name<-series_name
  input_test1$brand_name[grep("奔奔MINI",input_test1$model_name)]<-"长安轿车"
  input_test1$brand_name[grep("瑞风S2MINI",input_test1$model_name)]<-"江淮"
  input_test1$brand_name[grep("新豹MINI",input_test1$model_name)]<-"长安商用"
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
  a1<-inner_join(input_test1,rm_series_rule,by="qx_series_all")%>%
    dplyr::select(car_id,id)
  a2<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a1$car_id)))
  input_test1<-inner_join(input_test1,a2,by="car_id")%>%dplyr::select(-qx_series_all)
  ####################################第二轮：系列名匹配######################
  a2<-inner_join(input_test1,rm_series_rule,c("series_name"="series"))%>%
    dplyr::select(car_id,id)
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")
  data_input_0<-rbind(a1,a2)
  data_input_0<-inner_join(data_input_0,yck_yiche,by="car_id")
  data_input_0<-inner_join(data_input_0,rm_series_rule,by="id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name,model_price)
  data_input_0<-rbind(data_input_0,input_test1)
  linshi<-str_extract(data_input_0$model_name,"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  data_input_0$series_name<-paste(data_input_0$series_name,linshi,sep = "")
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0<-data.frame(data_input_0,discharge_standard="",liter="",auto="")
  #---准确性accurate
  accurate<-c(nrow(a3),nrow(yck_yiche))
  rm(a1,a2,a3,input_test1,qx_series_all,qx_series_des,linshi,linshi_series,brand_name)
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
  car_year<-str_extract(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
  car_year<-gsub("款","",car_year)
  loc_year<-str_locate(qx_name,c(str_c(1990:2030,"款",sep="",collapse='|')))
  car_price<-round(data_input$model_price/10000,2)
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
  qx_name<-gsub("格锐","格越",qx_name)
  qx_name<-gsub("北京现代","现代",qx_name)
  qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)
  qx_name<-fun_normalization(qx_name)
  
  ###
  car_name<-data_input$brand_name
  car_series1<-data_input$series_name
  car_series1<-gsub("\\（","(",car_series1)
  car_series1<-gsub("\\）",")",car_series1)
  car_series1<-gsub("\\(进口\\)|\\(海外\\)|-进口|-海外","",car_series1)
  qx_name<-paste(car_name,car_series1,qx_name,sep = "")
  seriesFun<-function(i){
    sub(car_name[i],"",car_series1[i])
  }
  series_des<-unlist(lapply(1:length(car_name),seriesFun))
  series_des<-trim(series_des)
  series_all<-paste(car_name,series_des,sep = "")
  ##1、清洗全称
  seriesFun<-function(i){
    gsub(series_all[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_name),seriesFun))
  ##2、清洗系列
  seriesFun<-function(i){
    gsub(car_series1[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_name),seriesFun))
  ##3、清洗品牌
  seriesFun<-function(i){
    gsub(car_name[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_name),seriesFun))
  
  car_name<-data_input$brand_name
  car_series1<-data_input$series_name
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
  qx_yiche<-data.frame(X= data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_yiche<-trim(qx_yiche)
  qx_yiche$car_model_name<-gsub(" +"," ",qx_yiche$car_model_name)
  qx_yiche<-sapply(qx_yiche,as.character)
  for (i in 1:dim(qx_yiche)[2]) {
    qx_yiche[,i][which(is.na(qx_yiche[,i]))]<-""
  }
  qx_yiche<-data.frame(qx_yiche)
  qx_yiche$X<-as.integer(as.character(qx_yiche$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_yiche$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##第二大章：数据匹配##
  data_input<-qx_yiche
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " '' state,n.trans_fee,'' transfer,n.annual,insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='yiche' AND match_des='right') m",
                                                       " INNER JOIN spider_www_yiche n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    #######location清洗&nbsp
    input_orig$location<-gsub("巢湖","合肥",input_orig$location)
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    location_ls[which(is.na(location_ls))]<-str_extract(input_orig$location[which(is.na(location_ls))],paste0(unique(config_distr$province),sep="",collapse = "|"))
    location_ls<-data.frame(city=location_ls)
    location_ls<-left_join(location_ls,config_distr,c("city"="city"))
    location_ls$province[which(is.na(location_ls$province))]<-location_ls$city[which(is.na(location_ls$province))]
    location_ls$regional[which(is.na(location_ls$regional))]<-
      as.character(left_join(location_ls[which(is.na(location_ls$regional)),],unique(config_distr[,1:2]),by="province")$regional.y)
    input_orig$location<-location_ls$city
    input_orig<-data.frame(input_orig,regional=location_ls$regional,province=location_ls$province)
    if(length(which(is.na(location_ls$regional)))>0){input_orig<-input_orig[-which(is.na(location_ls$regional)),]}
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
    input_orig$annual<-gsub("车主未填写|年|- -","",input_orig$annual)
    input_orig$annual<-gsub("月","01",input_orig$annual)
    input_orig$annual<-gsub("过保|已过期|已到期","19800101",input_orig$annual)
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual,format = "%Y%m%d")
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险
    input_orig$insure<-gsub("车主未填写|年|- -","",input_orig$insure)
    input_orig$insure<-gsub("月","01",input_orig$insure)
    input_orig$insure<-gsub("过保|已过期|已到期","19800101",input_orig$insure)
    input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure,format = "%Y%m%d")
    input_orig$insure[which(input_orig$insure>0)]<-1
    input_orig$insure[which(input_orig$insure<=0)]<-0
    input_orig$insure<-factor(input_orig$insure)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result_yiche(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="yiche",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\yiche.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='yiche'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/yiche.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/yiche.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='yiche'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
}
washfun_youxin<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  # field.name<-dbListFields(loc_channel,"")
  yck_youxin<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,model model_name,emission discharge_standard,gearbox car_auto FROM spider_www_xin a
                                WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('youxin'));"),-1)
  dbDisconnect(loc_channel)
  yck_youxin<-data.frame(car_id=yck_youxin$car_id,brand_name="",series_name="",yck_youxin[,-1],model_price="")
  yck_youxin$discharge_standard<-gsub("\\(.*","",yck_youxin$discharge_standard)
  yck_youxin$model_name[grep("双离合|G-DCT|DCT|DSG",yck_youxin$car_auto)]<-
    paste0(yck_youxin$model_name[grep("双离合|G-DCT|DCT|DSG",yck_youxin$car_auto)]," 双离合",sep="")
  
  ######################------第一部分：得到车的品牌及车系-------#################
  ##临时车名
  input_test1<-yck_youxin
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
  car_name_info<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$qx_series_all,sep="",collapse = "|")))
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
  data_input_0<-inner_join(data_input_0,yck_youxin[,c("car_id","brand_name","series_name","model_name")],by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name.y,model_price,qx_series_all,discharge_standard,auto=car_auto)
  data_input_0$model_name<-toupper(data_input_0$model_name)
  data_input_0$auto<-gsub("-／","",data_input_0$auto)
  data_input_0<-data.frame(data_input_0,liter="")
  #---准确性accurate
  accurate<-c(length(a4),nrow(yck_youxin))
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
  qx1_wutb<-gsub("长安","",qx1_wutb)
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
  
  
  ##------------停用词清洗--#-------词语描述归一
  car_series1<<- car_series1
  car_year<<- car_year
  car_model_name<<- car_model_name
  car_name<<-car_name
  car_price<<-car_price
  output_data<-fun_stopWords(data_input,qx_name)
  rm(car_series1,car_year,car_model_name,car_name,car_price);gc()
  #------------------数据保存----------------
  qx_youxin<-data.frame(X=data_input_0$car_id,output_data)
  
  #清洗多余空格
  qx_youxin<-trim(qx_youxin)
  qx_youxin$car_model_name<-gsub(" +"," ",qx_youxin$car_model_name)
  qx_youxin<-sapply(qx_youxin,as.character)
  for (i in 1:dim(qx_youxin)[2]) {
    qx_youxin[,i][which(is.na(qx_youxin[,i]))]<-""
  }
  qx_youxin<-data.frame(qx_youxin)
  qx_youxin$X<-as.integer(as.character(qx_youxin$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_youxin$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  ##第二大章：数据匹配##
  data_input<-qx_youxin
  
  #step2 function#
  fun_step2<-function(){
    loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
    dbSendQuery(loc_channel,'SET NAMES gbk')
    input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.address location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                       " n.state,'1' trans_fee,'' transfer,n.annual,n.insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                       " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='youxin' AND match_des='right' AND date_add='",data_new,"') m",
                                                       " INNER JOIN spider_www_xin n ON m.id_data_input=n.id",
                                                       " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
    dbDisconnect(loc_channel)
    ######
    input_orig$add_time<-as.Date(input_orig$add_time)
    input_orig$mile<-round(input_orig$mile/10000,2)
    input_orig$quotes<-round(input_orig$quotes/10000,2)
    ###location清洗
    location_ls<-str_extract(input_orig$location,paste0(config_distr$city,sep="",collapse = "|"))
    location_ls1<-data.frame(key_county=str_extract(input_orig$location[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
    location_ls1$key_county<-as.character(location_ls1$key_county)
    location_ls[which(is.na(location_ls))]<-as.character(inner_join(config_distr_all,location_ls1,by="key_county")$city)
    input_orig$location<-location_ls
    input_orig<-inner_join(input_orig,config_distr,c("location"="city"))
    #
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
    input_orig$annual<-as.Date(input_orig$add_time)-as.Date(input_orig$annual)
    input_orig$annual[which(input_orig$annual>0)]<-1
    input_orig$annual[which(input_orig$annual<=0)]<-0
    input_orig$annual<-factor(input_orig$annual)
    ##保险
    input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
    input_orig$insure[which(input_orig$insure>0)]<-1
    input_orig$insure[which(input_orig$insure<=0)]<-0
    input_orig$insure<-factor(input_orig$insure)
    ##分区字段(放最后)
    input_orig<-data.frame(input_orig,partition_month=format(input_orig$add_time,"%Y%m"))
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
    ##处理
    wutb<-wutb%>%dplyr::filter(round(quotes/model_price,2)>0.05&round(quotes/model_price,2)<1.01)
    wutb<-wutb%>%dplyr::filter(user_years<1|round(quotes/model_price,2)<1)
    wutb<-wutb%>%dplyr::filter(as.character(regDate)>'2000-01-01'&!is.na(regDate))
    fun_mysqlload_add(local_file,local_defin_yun,wutb,'analysis_wide_table')
  }
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,data_input)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>0){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform="youxin",return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\youxin.csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        accurate=round(accurate[2]/(accurate[1]+accurate[2]),3),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,"DELETE FROM analysis_match_id_temp WHERE car_platform='youxin'")
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/youxin.csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/youxin.csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='youxin'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      fun_step2()
    }
  }
}

washfun_che168()
washfun_che58()
washfun_csp()
washfun_czb()
washfun_guazi()
washfun_rrc()
washfun_youxin()
washfun_yiche()