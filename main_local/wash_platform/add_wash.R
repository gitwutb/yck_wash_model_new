##排除已匹配ID，并写入临时表(spider_www_chesupai替换数据采集表)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id id_data_input FROM spider_www_chezhibao;"),-1)
dbDisconnect(loc_channel)
loc_channel<-dbConnect(MySQL(),user = local_defin_yun$user,host=local_defin_yun$host,password= local_defin_yun$password,dbname=local_defin_yun$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
wide_id<-dbFetch(dbSendQuery(loc_channel,"SELECT id_data_input FROM analysis_wide_table where car_platform='czb';"),-1)
dbDisconnect(loc_channel)
df_id<-data.frame(id=setdiff(deal_data_input$id_data_input,wide_id$id_data_input)) %>% unique()
fun_mysqlload_all(local_file,local_defin,df_id,'temp_wide_id')
#取出未匹配的数据(替换查询washfun_czb)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
deal_data_input<-dbFetch(dbSendQuery(loc_channel,"SELECT a.id car_id,brand brand_name,series series_name,model model_name_t,'' model_price,'' model_year,
displacement liter,gearbox car_auto,emission discharge_standard FROM spider_www_chezhibao a
                          INNER JOIN temp_wide_id b ON a.id=b.id;"),-1)
dbDisconnect(loc_channel)


nrw<-nrow(deal_data_input)%/%30000
for (i in 1:nrw) {
  temp_deal_data_input<-deal_data_input[(30000*(i-1)+1):(30000*i),]
  washfun_UserCar_standard(temp_deal_data_input,'csp')
  print(i)
}
temp_deal_data_input<-deal_data_input[(30000*nrw+1):nrow(deal_data_input),]
washfun_UserCar_standard(temp_deal_data_input,'czb')
