fun_match_result<-function(che300,data_input){
  qx_che300<-che300
  qx_data_input<-data_input
  qx_che300$car_liter_wu[-grep("",qx_che300$car_liter_wu)]<-""
  qx_data_input$car_liter_wu[-grep("",qx_data_input$car_liter_wu)]<-""
  wutb<-NULL
  confidence<-NULL
  
  #-----第一部分0：年款、品牌车系、指导价--------#
  req_list<-fun_iteration(wutb,confidence,qx_data_input,qx_che300,c(2,5,14,25))
  req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,c(2,5,25))
  
  ###########-----第一部分1：使用固有国标字段--------######
  a<-c(4:21)
  req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,c(a[-17],24))
  req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,a)
  ###########-----第一部分3：全部字段：清除排量停用词--------######
  req_list$qx_data_input$car_liter_wu<-gsub("[A-Za-z]","",req_list$qx_data_input$car_liter_wu)
  req_list$qx_che300$car_liter_wu<-gsub("[A-Za-z]","",req_list$qx_che300$car_liter_wu)
  req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,c(a[-17],24))
  req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,a)
  
  ###########-----第二部分：减少一个字段--------######
  for (i in c(1,18:7,3)) {
    req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,a[-i])
  }
  
  ###########-----第三部分：减少不重要的几个字段--------######
  for (i in 21:9) {
    x<-c(4:i)
    req_list<-fun_iteration(req_list$wutb,req_list$confidence,req_list$qx_data_input,req_list$qx_che300,x)
  }
  wutb<-req_list$wutb
  
  ###########-----第二轮：第一部分：提升准确率(wutb中出现两次及以上即存在误差)--------######
  qx_che300<-che300
  qx_data_input<- data_input
  qx_che300$car_liter_wu[-grep("",qx_che300$car_liter_wu)]<-""
  qx_data_input$car_liter_wu[-grep("",qx_data_input$car_liter_wu)]<-""
  ##获取超过两次的data_input——ID
  wutb_id<-as.data.frame(table(wutb$id_data_input))%>%filter(Freq>=2)%>%dplyr::select(wutb_id=Var1)
  linshi_wutbid<-data.frame(wutb_id=setdiff(qx_data_input$X,unique(wutb$id_data_input)))
  wutb_id$wutb_id<-as.integer(as.character(wutb_id$wutb_id))
  wutb_id<-rbind(wutb_id,linshi_wutbid)
  qx_data_input<-inner_join(qx_data_input,wutb_id,c("X"="wutb_id"))
  ###---匹配---
  comp_result<-NULL
  for (i in 16:9) {
    x<-c(4:i,24)
    req_list<-fun_iteration(comp_result,confidence,qx_data_input,qx_che300,x)
    comp_result<-req_list$wutb
    qx_data_input<-req_list$qx_data_input
    qx_che300<-req_list$qx_che300
  }
  for (i in 16:9) {
    x<-c(4:i)
    req_list<-fun_iteration(comp_result,confidence,qx_data_input,qx_che300,x)
    comp_result<-req_list$wutb
    qx_data_input<-req_list$qx_data_input
    qx_che300<-req_list$qx_che300
  }
  ##-----总体可信度---
  linshi_rid<-data.frame(id_data_input=setdiff(wutb$id_data_input,unique(comp_result$id_data_input)))
  wutb<-inner_join(wutb,linshi_rid,by="id_data_input")
  wutb<-rbind(wutb,comp_result)
  linshicon<-c(length(unique(wutb$id_data_input))/length(wutb$id_data_input),length(unique(wutb$id_data_input)),length(wutb$id_data_input))
  confidence<-rbind(confidence,linshicon)
  
  ###########-----第二轮：第二部分：关键字段匹配--------##########################
  qx_che300<-che300
  qx_data_input<- data_input
  qx_che300$car_liter_wu[-grep("",qx_che300$car_liter_wu)]<-""
  qx_data_input$car_liter_wu[-grep("",qx_data_input$car_liter_wu)]<-""
  ##获取超过两次的data_input——ID
  wutb_id<-as.data.frame(table(wutb$id_data_input))%>%filter(Freq>=2)%>%dplyr::select(wutb_id=Var1)
  linshi_wutbid<-data.frame(wutb_id=setdiff(qx_data_input$X,unique(wutb$id_data_input)))
  wutb_id$wutb_id<-as.integer(as.character(wutb_id$wutb_id))
  wutb_id<-rbind(wutb_id,linshi_wutbid)
  qx_data_input<-inner_join(qx_data_input,wutb_id,c("X"="wutb_id"))
  comp_result<-NULL
  x<-c(5,7,9,10,4,12,6,11,13:21,24)
  for (i in 18:4) {
    req_list<-fun_iteration(comp_result,confidence,qx_data_input,qx_che300,x[-i])
    comp_result<-req_list$wutb
    qx_data_input<-req_list$qx_data_input
    qx_che300<-req_list$qx_che300
  }
  for (i in 18:5) {
    req_list<-fun_iteration(comp_result,confidence,qx_data_input,qx_che300,x[1:i])
    comp_result<-req_list$wutb
    qx_data_input<-req_list$qx_data_input
    qx_che300<-req_list$qx_che300
  }
  ###-----总体可信度--------
  linshi_rid<-data.frame(id_data_input=setdiff(wutb$id_data_input,unique(comp_result$id_data_input)))
  wutb<-inner_join(wutb,linshi_rid,by="id_data_input")
  wutb<-rbind(wutb,comp_result)
  
  ###########-----第二轮：第三部分：清除进口字段---##############
  qx_che300<-che300
  qx_data_input<- data_input
  qx_che300$car_liter_wu[-grep("",qx_che300$car_liter_wu)]<-""
  qx_data_input$car_liter_wu[-grep("",qx_data_input$car_liter_wu)]<-""
  ##获取超过两次的data_input——ID
  wutb_id<-as.data.frame(table(wutb$id_data_input))%>%filter(Freq>=2)%>%dplyr::select(wutb_id=Var1)
  linshi_wutbid<-data.frame(wutb_id=setdiff(qx_data_input$X,unique(wutb$id_data_input)))
  wutb_id$wutb_id<-as.integer(as.character(wutb_id$wutb_id))
  wutb_id<-rbind(wutb_id,linshi_wutbid)
  qx_data_input<-inner_join(qx_data_input,wutb_id,c("X"="wutb_id"))
  
  wutb_id300<-data.frame(wutb_id=unique(wutb[which(as.data.frame(table(wutb$id_data_input))[,2]==1),"id_che300"]))
  wutb_id300<-data.frame(wutb_id=setdiff(qx_che300$car_id,unique(wutb_id300$wutb_id)),stringsAsFactors = F)
  qx_che300<-inner_join(qx_che300,wutb_id300,c("car_id"="wutb_id"))
  ####---匹配-------
  qx_data_input$car_series1<-gsub("-进口","",qx_data_input$car_series1)
  qx_che300$car_series1<-gsub("-进口","",qx_che300$car_series1)
  comp_result<-NULL
  req_list<-fun_iteration(comp_result,confidence,qx_data_input,qx_che300,c(2,5,14,25))
  for (i in 16:9) {
    x<-c(4:i)
    req_list<-fun_iteration(req_list$wutb,confidence,req_list$qx_data_input,req_list$qx_che300,x)
  }
  x<-c(4:11,13:21)
  for (i in 17:10) {
    req_list<-fun_iteration(req_list$wutb,confidence,req_list$qx_data_input,req_list$qx_che300,x[1:i])
  }
  x<-c(4:10,13:14,17)
  for (i in 9:10) {
    req_list<-fun_iteration(req_list$wutb,confidence,req_list$qx_data_input,req_list$qx_che300,x[-i])
  }
  ###-----总体可信度--------
  linshi_rid<-data.frame(id_data_input=setdiff(wutb$id_data_input,unique(comp_result$id_data_input)))
  wutb<-inner_join(wutb,linshi_rid,by="id_data_input")
  wutb<-rbind(wutb,req_list$wutb)
  
  ###############----------------------跨平台数据配置输出return_db--------------##############
  qx_data_input<- data_input
  qx_che300<-che300
  ##没有匹配到的数据
  linshi_rid<-data.frame(X=setdiff(qx_data_input$X,unique(wutb$id_data_input)))
  match_not<-inner_join(qx_data_input,linshi_rid,by="X")
  if(nrow(match_not)==0){match_not<-NULL
  }else{match_not<-data.frame(match_des="not",id_data_input=match_not$X,id_che300="",brand=match_not$car_name,series=match_not$car_series1)}
  ##匹配多次的数据
  linshi_rid<-as.data.frame(table(wutb$id_data_input))%>%filter(Freq>=2)%>%dplyr::select(linshi_rid=Var1)
  linshi_rid$linshi_rid<-as.integer(as.character(linshi_rid$linshi_rid))
  match_repeat<-inner_join(wutb,linshi_rid,c("id_data_input"="linshi_rid"))%>%dplyr::select(id_data_input=id_data_input,id_che300)
  match_repeat<-inner_join(qx_che300,match_repeat,c("car_id"="id_che300"))
  if(nrow(match_repeat)==0){match_repeat<-NULL
  }else{match_repeat<-data.frame(match_des="repeat",id_data_input=match_repeat$id_data_input,id_che300=match_repeat$car_id,brand=match_repeat$car_name,series=match_repeat$car_series1)}
  match_repeat<-unique(match_repeat)
  ##匹配一次（正确）的数据
  linshi_rid<-as.data.frame(table(wutb$id_data_input))%>%filter(Freq==1)%>%dplyr::select(linshi_rid=Var1)
  linshi_rid$linshi_rid<-as.integer(as.character(linshi_rid$linshi_rid))
  match_right<-inner_join(wutb,linshi_rid,c("id_data_input"="linshi_rid"))%>%dplyr::select(id_data_input=id_data_input,id_che300)
  match_right<-inner_join(qx_che300,match_right,c("car_id"="id_che300"))
  if(nrow(match_right)==0){match_right<-NULL
  }else{match_right<-data.frame(match_des="right",id_data_input=match_right$id_data_input,id_che300=match_right$car_id,brand=match_right$car_name,series=match_right$car_series1)}
  return_db<-rbind(match_right,match_repeat,match_not)
  return(list(match_not=match_not,match_repeat=match_repeat,match_right=match_right,return_db=return_db))
}