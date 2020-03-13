##**************************第一部分：投诉部分
#车质网12365
fun_else_sync_12365<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM spider_complain_12365auto WHERE add_time>'",as.character(Sys.Date()-31),"';")) %>% dplyr::select(-url)
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_12365auto")
  return(1)
}
#车主之家投诉
fun_else_sync_autoownerComp<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT complain_id,series_id,city,tag1,des1,tag2,des2,tag3,des3,tag4,des4,tag5,des5,tag6,des6,
                                                     DATE_FORMAT(complain_time,'%Y-%m-%d') complain_time,DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                                                     FROM spider_complain_autoowner WHERE DATE_FORMAT(update_time,'%Y-%m-%d')>'",as.character(Sys.Date()-31),"';"))
  location_ls<-str_extract(output_save$city,paste0(config_distr$city,sep="",collapse = "|"))
  location_ls1<-data.frame(key_county=str_extract(output_save$city[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
  location_ls1$key_county<-as.character(location_ls1$key_county)
  location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
  output_save$city<-location_ls
  output_save<-inner_join(output_save,config_distr,c("city"="city")) %>% 
    dplyr::select(complain_id,series_id,province,city,tag1,des1,tag2,des2,tag3,des3,tag4,des4,tag5,des5,tag6,des6,complain_time,update_time)
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_autoowner")
  return(1)
}
#315汽车
fun_else_sync_315qc<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT id,series,brand,question,complain_time,DATE_FORMAT(add_time,'%Y-%m-%d') add_time
                                                      FROM spider_complain_315qc WHERE DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(Sys.Date()-31),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_315qc")
  return(1)
}
#汽车投诉网
fun_else_sync_qctsw<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT id,series,brand,car_type,car_status,appeal,question,buy_time,complain_time,DATE_FORMAT(add_time,'%Y-%m-%d') add_time
                                                      FROM spider_complain_qctsw WHERE DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(Sys.Date()-31),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_qctsw")
  return(1)
}
#汽车门
fun_else_sync_qichemen<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT id,series,brand,tag1,tag2,tag3,complain_time,DATE_FORMAT(add_time,'%Y-%m-%d') add_time
                                                      FROM spider_complain_qichemen WHERE DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(Sys.Date()-31),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_qichemen")
  return(1)
}

if(as.integer(format(Sys.Date(),"%d"))==1){
  if(tryCatch({fun_else_sync_12365(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：12365')}
  if(tryCatch({fun_else_sync_autoownerComp(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：车主之家')}
  if(tryCatch({fun_else_sync_315qc(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：315汽车')}
  if(tryCatch({fun_else_sync_qctsw(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：汽车投诉')}
  if(tryCatch({fun_else_sync_qichemen(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：汽车门')}
}


##**************************第二部分：经销商报价：汽车之家（本地20/21号抓完，24号开始同步）
fun_else_sync_autoDiscount<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM discount_rate_history WHERE stat_time='",paste0(str_sub(as.character(Sys.Date()),1,8),'15'),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"discount_rate_history")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'经销商报价-discount_rate_history表更新完毕')}
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==28){
  if(tryCatch({fun_else_sync_autoDiscount(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'经销商报价同步失败：汽车之家')}
}

##**************************第三部分：汽车销量（本地25号抓完，26号开始同步）
##搜狐
fun_else_sync_souhuSalenumber<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT * FROM spider_salesnum_souhu WHERE add_time>'",paste0(str_sub(as.character(Sys.Date()),1,8),'01'),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_salesnum_souhu")
  return(1)
}
##车主之家
fun_else_sync_autoownerSalenumber<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("SELECT series_id,stat_date,salesNum,update_time add_time FROM spider_salesnum_autoowner WHERE update_time>'",paste0(str_sub(as.character(Sys.Date()-31),1,8),'01'),"';"))
  config_series_brand<-fun_mysqlload_query(local_defin,"select DISTINCT series_id,series_name,brand_id,brand_name from config_autoowner_major_info_tmp ;")
  output_save$stat_date<-paste0(as.character(output_save$stat_date),'-01') 
  output_save$stat_date<-as.Date(output_save$stat_date )
  config_series_brand$series_id<-as.numeric(config_series_brand$series_id)
  config_series_brand$brand_id<-as.numeric(config_series_brand$brand_id)
  output_save<-output_save %>% dplyr::inner_join(config_series_brand,by ="series_id") %>%
    dplyr::select(brand_id,brand_name,series_id,series_name,stat_date,salesNum,add_time)
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_salesnum_autoowner")
  return(1)
}

if(as.integer(format(Sys.Date(),"%d"))==26){
  if(tryCatch({fun_else_sync_souhuSalenumber(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'销量数据同步失败：搜狐')}
  if(tryCatch({fun_else_sync_autoownerSalenumber(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'销量数据同步失败：车主之家')}
}

##**************************第四部分：汽车口碑：汽车之家（每月一号同步）
fun_else_sync_autoKoub<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT id,eid,model_id,isbattery,IF(drivenKilometers_appends>=drivekilometer,drivenKilometers_appends,drivekilometer) miles,
                                        boughtdate,SUBSTR(boughtcity_id FROM 1 FOR 2) province_id,SUBSTR(boughtcity_id FROM 1 FOR 4) city_id,visitcount,helpfulcount,commentcount,boughtPrice,
                                        score_spaceScene space,score_powerScene power,score_maneuverabilityScene control,score_oilScene oilconsumption,score_batteryScene eleconsumption ,
                                        score_comfortablenessScene comfortableness,score_apperanceScene apperance,score_internalScene interior,score_costefficientScene costefficient,satisfaction,
                                        append_time comment_time,DATE_FORMAT(add_time,'%Y-%m-%d') add_time from spider_koubei_autohome WHERE DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-5,"%Y-%m-01")),"';")),-1)
  config_model_info<-dbFetch(dbSendQuery(loc_channel,"SELECT model_id,model_name,brand_id,brand_name,series_id,series_name,model_year,model_price from config_autohome_major_info_tmp;"),-1)
  config_district_code<-dbFetch(dbSendQuery(loc_channel,"select SUBSTR(city_code,1,2) province_id,SUBSTR(city_code,1,4) city_id,key_province province,min(key_municipal) city 
             from config_district  group by SUBSTR(city_code,1,4), key_province;"),-1)
  #问题：有些省存在直辖县，如湖北、海南、新疆            
  dbDisconnect(loc_channel)
  config_model_info$model_id<-as.numeric(config_model_info$model_id)
  config_district_codeP<-config_district_code %>% dplyr::select(province_id,province) %>% unique()
  config_district_codeC<-config_district_code %>% dplyr::select(city_id,city) %>% unique()
  output_save<-output_save %>% dplyr::inner_join(config_model_info,by='model_id') %>%
    dplyr::inner_join(config_district_codeP,by='province_id') %>% dplyr::left_join(config_district_codeC,by='city_id')
  output_save$boughtdate<-paste0(as.character(output_save$boughtdate),'-01')
  output_save<- output_save %>% 
    dplyr::select(id,eid,model_id,brand_id,brand_name,series_id,series_name,model_year,model_name,model_price,isbattery,
                  boughtdate,boughtPrice,province,city,miles,visitcount,helpfulcount,commentcount,space,power,control,
                  oilconsumption,eleconsumption,comfortableness,apperance,interior,costefficient,satisfaction,comment_time,add_time)
  fun_mysqlload_add_upd(local_file,local_defin_yun,output_save,"spider_koubei_autohome")
}
if(as.integer(format(Sys.Date(),"%d"))==1){
  if(tryCatch({fun_else_sync_autoKoub(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'口碑数据同步失败：汽车之家')}
 }

##**************************第五部分：经销商信息（每周更新，每月1号同步）
##汽车之家
fun_else_sync_autohomeDealer<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("select id,dealer_id,shop_4s,name,brand,linkPhone,province_name,city_name,update_time
                                   from spider_dealer_autohome where DATE_FORMAT(update_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-5,"%Y-%m-01")),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_dealer_autohome")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'经销商-spider_dealer_autohome表更新完毕')}
  return(1)
}
##易车
fun_else_sync_yicheDealer<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("select id,dealer_id,shop_4s,name,brand,linkPhone,province_name,city_name,update_time
                         from spider_dealer_bitauto where DATE_FORMAT(update_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-5,"%Y-%m-01")),"';"))
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_dealer_bitauto")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'经销商-spider_dealer_bitauto表更新完毕')}
  return(1)
}
##车主之家
fun_else_sync_autoownerDealer<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("select id,dealer_id,shop_4s,name,brand,linkPhone,province_name,city_name,update_time
                        from spider_dealer_ownerhome  where DATE_FORMAT(update_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-5,"%Y-%m-01")),"';"))
  output_save$city_name<-gsub("市","",output_save$city_name)
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_dealer_ownerhome")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'经销商-spider_dealer_ownerhome表更新完毕')}
  return(1)
}

if(as.integer(format(Sys.Date(),"%d"))==1){
  if(tryCatch({fun_else_sync_autohomeDealer(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'经销商信息同步失败：汽车之家')}
  if(tryCatch({fun_else_sync_yicheDealer(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'经销商信息同步失败：易车')}
  if(tryCatch({fun_else_sync_autoownerDealer(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'经销商信息同步失败：车主之家')}
}

##************************第六部分：车主裸车价（每周同步，ps：易车裸车价还未爬完，爬取数据为易车app）

#######***********************汽车之家车主裸车价(同步)************************###########################
fun_else_sync_autohomePrice<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("select id,model_id,series_id,series_name,model_name,boughtaddress city,boughtdate,owner_price_id owner_id,model_price,bare_price,fullprice,
                                                      purchase_tax,commercial_insure,vehicle_tax,high_insure,card_fee,add_time from spider_ownerprice_autohome
                                                      where DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-5,"%Y-%m-01")),"';"))
  config_distr<<-fun_mysqlload_query(local_defin,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                                  INNER JOIN config_district_regional b ON a.key_province=b.province;")
  config_distr_all<<-fun_mysqlload_query(local_defin,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                                  INNER JOIN config_district_regional b ON a.key_province=b.province where a.key_county!='河南';")
  output_save$boughtdate<-as.Date(output_save$boughtdate)
  province_ls<-str_extract(output_save$city,paste0(config_distr$province,sep="",collapse = "|"))
  city_ls<-str_extract(output_save$city,paste0(config_distr$city,sep="",collapse = "|"))
  city_ls1<-data.frame(key_county=str_extract(output_save$city[which(is.na(city_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
  city_ls1$key_county<-as.character(city_ls1$key_county)
  city_ls[which(is.na(city_ls))]<-as.character(right_join(config_distr_all,city_ls1,by="key_county")$city) 
  output_save$city<-city_ls
  output_save$province<-province_ls
  output_save<-output_save %>% filter(model_price!=0,bare_price!=0) %>% 
    mutate(error1=abs(model_price-bare_price)/bare_price,error2=abs(model_price-bare_price)/model_price) %>%
    filter(error1<=3,error2<=2) %>% 
    dplyr::select(id,model_id,series_id,series_name,model_name,model_price,province,city,boughtdate,owner_id,
                  bare_price,fullprice,purchase_tax,commercial_insure,vehicle_tax,high_insure,card_fee,add_time)
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_ownerprice_autohome")
  return(1)
}

if(weekdays(Sys.Date())=='星期四'){
  if(tryCatch({fun_else_sync_autohomePrice(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'车主价格数据同步失败：汽车之家')}
}

#######***********************易车车主裸车价(同步)************************###########################
fun_else_sync_yichePrice<-function(input_ip){
  output_save<-fun_mysqlload_query(local_defin,paste0("select id,brand_name,series_name,model_name,buy_time,city,
                                                     ROUND(guidance_price/10000,2) model_price,ROUND(naked_price/10000,2) bare_price,add_time from spider_nakedprice_yiche
                                                     WHERE DATE_FORMAT(add_time,'%Y-%m-%d')>'",as.character(format.Date(Sys.Date()-8,"%Y-%m-20")),"';"))
  config_distr<<-fun_mysqlload_query(local_defin,"SELECT DISTINCT regional,b.province,a.key_municipal city FROM config_district a
                                                  INNER JOIN config_district_regional b ON a.key_province=b.province;")
  config_distr_all<<-fun_mysqlload_query(local_defin,"SELECT DISTINCT regional,b.province,a.key_municipal city,a.key_county FROM config_district a
                                                  INNER JOIN config_district_regional b ON a.key_province=b.province;")
  output_save$buy_time<-as.Date(output_save$buy_time)
  #通过区县
  location_ls<-str_extract(output_save$city,paste0(config_distr$city,sep="",collapse = "|"))
  location_ls1<-data.frame(key_county=str_extract(output_save$city[which(is.na(location_ls))],paste0(config_distr_all$key_county,sep="",collapse = "|")))
  location_ls1$key_county<-as.character(location_ls1$key_county)
  location_ls[which(is.na(location_ls))]<-as.character(right_join(config_distr_all,location_ls1,by="key_county")$city)
  output_save$city<-location_ls
  output_save<-inner_join(output_save,config_distr,by="city")
  output_save<-output_save %>% dplyr::select(-regional)
  ##获取年款
  output_save$model_year<-str_extract(output_save$model_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  output_save$model_year<-gsub("款","",output_save$model_year)
  output_save$brand_name<-toupper(output_save$brand_name)
  output_save$series_name<-toupper(output_save$series_name)
  output_save<-output_save %>% mutate(error1=abs(model_price-bare_price)/bare_price,error2=abs(model_price-bare_price)/model_price) %>%
    filter(error1<=3,error2<=2) %>% dplyr::select(id,brand_name,series_name,model_year,model_name,model_price,province,city,buy_time,bare_price,add_time)
  fun_mysqlload_add_upd(local_file,local_defin_yun,output_save,'spider_nakedprice_yiche')
  return(1)
}

if(as.integer(format(Sys.Date(),"%d"))==24){
  if(tryCatch({fun_else_sync_yichePrice(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'车主价格同步失败：易车')}
}

