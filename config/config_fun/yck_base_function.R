###*******基础库******##
#库1：邮箱发送
fun_mailsend<-function(input_subject,input_body){
  send.mail(from = "270437211@qq.com",
            to = c("270437211@qq.com"),
            subject = input_subject,
            encoding = 'utf-8',
            body = input_body,
            html = TRUE,
            smtp = list(host.name = "smtp.qq.com",port = 465,user.name = "270437211@qq.com",passwd = "lzbzotpxumvrbgbi",ssl = TRUE,tls =TRUE),
            authenticate = TRUE,
            send = TRUE)
}
#库2:数据插入到表
fun_mysqlload_add<-function(input_path,input_ip,input_table,input_tablename){
  file.remove(paste0(input_path,"/file/",input_tablename,".csv"))
  write.csv(input_table,paste0(input_path,"/file/",input_tablename,".csv"),
            row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = input_ip$user,host=input_ip$host,password= input_ip$password,dbname=input_ip$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(input_path,"/file/",input_tablename,".csv"),"'",
                                 " INTO TABLE ",input_tablename," CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}
fun_mysqlload_all<-function(input_path,input_ip,input_table,input_tablename){
  file.remove(paste0(input_path,"/file/",input_tablename,".csv"))
  write.csv(input_table,paste0(input_path,"/file/",input_tablename,".csv"),
            row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = input_ip$user,host=input_ip$host,password= input_ip$password,dbname=input_ip$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("TRUNCATE TABLE ",input_tablename))
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(input_path,"/file/",input_tablename,".csv"),"'",
                                 " INTO TABLE ",input_tablename," CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}
fun_mysqlload_add_upd<-function(input_path,input_ip,input_table,input_tablename){
  file.remove(paste0(input_path,"/file/",input_tablename,".csv"))
  write.csv(input_table,paste0(input_path,"/file/",input_tablename,".csv"),
            row.names = F,fileEncoding = "UTF-8",quote = F)
  loc_channel<-dbConnect(MySQL(),user = input_ip$user,host=input_ip$host,password= input_ip$password,dbname=input_ip$dbname)
  dbSendQuery(loc_channel,'SET NAMES gbk')
  dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE ","'",paste0(input_path,"/file/",input_tablename,".csv"),"'",
                                 " REPLACE INTO TABLE ",input_tablename," CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;"))
  dbDisconnect(loc_channel)
}