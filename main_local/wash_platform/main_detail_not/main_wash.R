###############################################综合所有的平台数据清洗程序############################
rm(list = ls(all=T))
gc()
##文件主路径（移动文件，变更下一行路径即可）
price_model_loc<-gsub("(\\/main|\\/bat).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
lf<-list.files(paste0(price_model_loc,"/file/output",sep=""), full.names = T,pattern = "csv")
file.remove(lf)
##删除##清除csv文件

###################第一部分（基本输出）：各个平台数据分析
car_platform<-c('rrc.R','che168.R','csp.R','che58.R','guazi.R','youxin.R','czb.R','yiche.R')
for (i in 1:length(car_platform)) {
  tryCatch({source(paste0(paste0(gsub("(\\/main|\\/bat).*","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()})),"/main_local/wash_platform/",sep=""),car_platform[i],sep=""),echo=TRUE,encoding="utf-8")},
           error=function(e){print(paste0("错误!"))},
           finally={print(paste0("进程完成!"))})
  car_platform<-c('rrc.R','che168.R','csp.R','che58.R','guazi.R','youxin.R','czb.R','yiche.R')
}
######暂停###
###source("E:/Work_table/Study/Rexam/YCK/wash_car_model/main/main_detail/souche.R",echo=TRUE,encoding="utf-8")



# ###############################################综合所有的平台数据清洗程序############################
# #清除缓存
# rm(list = ls(all=T))
# gc()
# ##清除csv文件
# lf<-list.files("E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output", full.names = T,pattern = "csv")
# file.remove(lf)
# ##删除
# lf<-list.files("E:/Work_table/gitwutb/git_project/yck_wash_car_model/file/output_final", full.names = T,pattern = "csv")
# file.remove(lf)
# 
# 
# ###################第一部分（基本输出）：各个平台数据分析
# car_platform<-c('rrc_not.R','che168_not.R','csp_not.R','che58_not.R','guazi_not.R','youxin_not.R','czb_not.R','yiche_not.R')
# for (i in 1:length(car_platform)) {
#   tryCatch({source(paste0("E:/Work_table/gitwutb/git_project/yck_wash_car_model/main/main_detail_not/",car_platform[i],sep=""),echo=TRUE,encoding="utf-8")},
#            error=function(e){cat(write.table(data.frame(platform=paste0('iserror'),data=Sys.Date()),
#                                              paste0(deep_local,"wash_car_model/file/output/rizhi_error.txt",sep=""),col.names = F,row.names = F,append=T),conditionMessage(e),"\n\n")},
#            finally={print(paste0("进程完成!"))})
#   car_platform<-c('rrc_not.R','che168_not.R','csp_not.R','che58_not.R','guazi_not.R','youxin_not.R','czb_not.R','yiche_not.R')
# }