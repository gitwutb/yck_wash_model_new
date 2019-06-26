##投诉部分：12365
fun_else_sync_12365<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT * FROM spider_complain_12365auto WHERE add_time>'",as.character(Sys.Date()-30),"';")),-1) %>% dplyr::select(-url)
  dbDisconnect(loc_channel)
  for (i in 1:dim(output_save)[2]) {
    output_save[,i][which(is.na(output_save[,i]))]<-"\\N"
    output_save[,i]<-gsub("\\,","，",output_save[,i])
    output_save[,i]<-gsub("\\\n","",output_save[,i])
  }
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_complain_12365auto")
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==1){
  if(tryCatch({fun_else_sync_12365()},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'投诉数据同步失败：12365')}
}

##经销商报价：汽车之家（本地20/21号抓完，24号开始同步）
fun_else_sync_autoDiscount<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT * FROM discount_rate_history WHERE stat_time='",paste0(str_sub(as.character(Sys.Date()),1,8),'15'),"';")),-1)
  dbDisconnect(loc_channel)
  for (i in 1:dim(output_save)[2]) {
    output_save[,i][which(is.na(output_save[,i]))]<-"\\N"
    output_save[,i]<-gsub("\\,","，",output_save[,i])
    output_save[,i]<-gsub("\\\n","",output_save[,i])
  }
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"discount_rate_history")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'经销商报价-discount_rate_history表更新完毕')}
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==24){
  if(tryCatch({fun_else_sync_autoDiscount(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'经销商报价同步失败：汽车之家')}
}


##汽车销量：搜狐（本地25号抓完，26号开始同步）
fun_else_sync_souhuSalenumber<-function(input_ip){
  loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  output_save<-dbFetch(dbSendQuery(loc_channel,paste0("SELECT * FROM spider_salesnum_souhu WHERE add_time>'",paste0(str_sub(as.character(Sys.Date()),1,8),'01'),"';")),-1)
  dbDisconnect(loc_channel)
  for (i in 1:dim(output_save)[2]) {
    output_save[,i][which(is.na(output_save[,i]))]<-"\\N"
    output_save[,i]<-gsub("\\,","，",output_save[,i])
    output_save[,i]<-gsub("\\\n","",output_save[,i])
  }
  fun_mysqlload_add_upd(local_file,input_ip,output_save,"spider_salesnum_souhu")
  if(nrow(output_save)>1){fun_mailsend("数据同步-其它部分",'销量数据-spider_salesnum_souhu表更新完毕')}
  return(1)
}
if(as.integer(format(Sys.Date(),"%d"))==26){
  if(tryCatch({fun_else_sync_souhuSalenumber(local_defin_yun)},error=function(e){0},finally={0})!=1){
    fun_mailsend("数据同步-其它部分",'销量数据同步失败：搜狐')}
}