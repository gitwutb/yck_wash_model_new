##第一类：公共函数
washfun_UserCar_standard<-function(deal_data_input,input_tname){
  deal_data_input<-dealFun_brandseries(deal_data_input)
  deal_data_input$model_price<-as.numeric(as.character(deal_data_input$model_price))
  #--停用词清洗--#-词语描述归一-
  output_data<-fun_stopWords(deal_data_input)
  qx_tablename<-data.frame(X=deal_data_input$car_id,output_data)
  #清洗多余空格
  qx_tablename<-trim(qx_tablename)
  qx_tablename$car_model_name<-gsub(" +"," ",qx_tablename$car_model_name)
  qx_tablename<-sapply(qx_tablename,as.character) %>% as.data.frame(stringsAsFactors=F)
  for (i in 1:dim(qx_tablename)[2]) {
    qx_tablename[,i][which(is.na(qx_tablename[,i]))]<-""
  }
  qx_tablename$X<-as.integer(as.character(qx_tablename$X))
  #剔除不包含的车系
  df_filter<- gsub('-进口','',unique(qx_tablename$car_series1)) %>% as.character()
  che300<-che300 %>% filter(gsub('-进口','',car_series1)%in%df_filter)
  
  #step3数据处理
  list_result<-tryCatch({fun_match_result(che300,qx_tablename)},
                        error=function(e){0},
                        finally={-1})
  if(length(list_result)>1){
    return_db<-list_result$return_db
    match_right<-list_result$match_right
    match_repeat<-list_result$match_repeat
    match_not<-list_result$match_not
    return_db<-data.frame(car_platform=input_tname,return_db) %>% dplyr::select(-brand,-series) %>% mutate(date_add=Sys.Date())
    return_db$id_che300<-as.integer(as.character(return_db$id_che300))
    return_db$id_che300[is.na(return_db$id_che300)]<-''
    #判别是否存在正确的匹配
    if(nrow(match_right)>0){
      write.csv(return_db,paste0(local_file,"\\file\\output\\",input_tname,".csv",sep=""),row.names = F,fileEncoding = "UTF-8",quote = F)
      rizhi<-data.frame(platform=unique(return_db$car_platform),
                        n_right=nrow(match_right),n_repeat=nrow(match_repeat),
                        n_not=nrow(match_not),add_date=Sys.Date())
      loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
      dbSendQuery(loc_channel,'SET NAMES gbk')
      dbSendQuery(loc_channel,paste0("DELETE FROM analysis_match_id_temp WHERE car_platform='",input_tname,"'",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/",input_tname,".csv'",
                                     " INTO TABLE analysis_match_id CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",local_file,"/file/output/",input_tname,".csv'",
                                     " INTO TABLE analysis_match_id_temp CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
      dbSendQuery(loc_channel,paste0("UPDATE analysis_match_idmax_temp SET max_id= '",max(return_db$id_data_input),"',date_add='",Sys.Date(),"' WHERE car_platform='",input_tname,"'"))
      dbWriteTable(loc_channel,"log_analysis_match_id",rizhi,append=T,row.names=F)
      dbDisconnect(loc_channel)
      eval(parse(text = paste0('fun_step2_',input_tname,"()")))
    }
  }
}
##第二类：独有治理函数
fun_step2_che168<-function(){
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
fun_step2_che58<-function(){
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure[grep("年[1-9]月",input_orig$insure)]<-gsub("年","年0",input_orig$insure[grep("年[1-9]月",input_orig$insure)])
  input_orig$insure<-gsub("车主未填写|年","",input_orig$insure)
  input_orig$insure<-gsub("月","01",input_orig$insure)
  input_orig$insure<-gsub("过保","19800101",input_orig$insure)
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure,format = "%Y%m%d")
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
fun_step2_csp<-function(){
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
fun_step2_czb<-function(){
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
  input_orig$annual<-as.character(input_orig$annual)
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
fun_step2_guazi<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'-' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                     " n.state,'0' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                     " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='guazi' AND match_des='right') m",
                                                     " INNER JOIN spider_www_guazi n ON m.id_data_input=n.id",
                                                     " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
  dbDisconnect(loc_channel)
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
fun_step2_rrc<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,n.color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                     " n.state,'1' trans_fee,n.transfer,n.annual,n.high_insure insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time,n.address",
                                                     " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='rrc' AND match_des='right') m",
                                                     " INNER JOIN spider_www_renren n ON m.id_data_input=n.id",
                                                     " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
  dbDisconnect(loc_channel)
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
fun_step2_yiche<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  input_orig<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT m.car_platform,m.id_data_input,m.id_che300,p.model_year,p.yck_brandid,p.yck_seriesid,p.is_import,p.is_green,p.brand_name brand,p.series_name series,p.model_name,'' color,p.liter,p.auto,p.discharge_standard,p.car_level,n.location,n.regDate,n.quotes,p.model_price,n.mile,",
                                                     " '' state,n.trans_fee,'' transfer,n.annual,insure,DATE_FORMAT(n.add_time,'%Y-%m-%d')  add_time,DATE_FORMAT(n.update_time,'%Y-%m-%d') update_time",
                                                     " FROM (SELECT car_platform,id_data_input,id_che300 FROM analysis_match_id_temp where car_platform='yiche' AND match_des='right') m",
                                                     " INNER JOIN spider_www_yiche n ON m.id_data_input=n.id",
                                                     " INNER JOIN config_vdatabase_yck_major_info p ON m.id_che300=p.model_id;")),-1)
  dbDisconnect(loc_channel)
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure<-gsub("车主未填写|年|- -","",input_orig$insure)
  input_orig$insure<-gsub("月","01",input_orig$insure)
  input_orig$insure<-gsub("过保|已过期|已到期","19800101",input_orig$insure)
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure,format = "%Y%m%d")
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
fun_step2_youxin<-function(){
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
  input_orig$annual<-as.character(input_orig$annual)
  ##保险
  input_orig$insure<-as.Date(input_orig$add_time)-as.Date(input_orig$insure)
  input_orig$insure[which(input_orig$insure>0)]<-1
  input_orig$insure[which(input_orig$insure<=0)]<-0
  input_orig$insure<-as.character(input_orig$insure)
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
##二手车价格平台处理函数
washfun_che168<-function(){
  #步骤一：拿取二手车信息
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name_t,'' model_price,'' model_year,
                              displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_168 a
                                WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('che168'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'che168')
  return(1)
}
washfun_che58<-function(){
  #步骤一
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  table.name<-dbListTables(loc_channel)
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name_t,model_price,'' model_year,
  displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_58 a
                                WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('che58'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'che58')
  return(1)
}
washfun_csp<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,'' brand_name,'' series_name,model model_name_t,'' model_price,'' model_year,
  '' liter,gearbox car_auto,emission discharge_standard FROM spider_www_chesupai a 
                             WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('csp'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'csp')
  return(1)
}
washfun_czb<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name_t,'' model_price,'' model_year,
  displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_chezhibao a
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('czb'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'czb')
  return(1)
}
washfun_guazi<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,'' brand_name,'' series_name,model model_name_t,'' model_price,'' model_year,
  displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_guazi a
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('guazi'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'guazi')
  return(1)
}
washfun_rrc<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name_t,ROUND(tax_price/10000,2) model_price,'' model_year,
                  displacement liter,'' car_auto,emission discharge_standard FROM spider_www_renren a 
                             WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('rrc'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'rrc')
  return(1)
}
washfun_yiche<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,brand brand_name,series series_name,model model_name_t,ROUND(msrp/10000,2) model_price,'' model_year,
  displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_yiche a 
                               WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('yiche'));"),-1)
  dbDisconnect(loc_channel)
  washfun_UserCar_standard(deal_data_input,'yiche')
  return(1)
}
washfun_youxin<-function(){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id car_id,'' brand_name,'' series_name,model model_name_t,ROUND(tax_price/10000,2) model_price,'' model_year,
        displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_xin a
                                WHERE a.id>(SELECT max_id FROM analysis_match_idmax_temp WHERE car_platform in ('youxin'));"),-1)
  dbDisconnect(loc_channel)
  deal_data_input$liter<-round(as.numeric(gsub(" |mL|ML|ml",'',deal_data_input$liter))/1000,1)
  washfun_UserCar_standard(deal_data_input,'youxin')
  return(1)
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