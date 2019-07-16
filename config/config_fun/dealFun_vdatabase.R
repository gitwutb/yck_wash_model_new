##函数：处理品牌/车系
dealFun_brandseries<-function(input_cxk){
  forFun<-function(i){
    sub(input_cxk$brand_name[i],"",input_cxk$series_name[i])
  }
  series_all<-unlist(lapply(1:length(input_cxk$series_name),forFun))
  linshi<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",input_cxk$model_name_t)
  input_cxk<-data.frame(input_cxk,model_name=paste0(input_cxk$brand_name,series_all,linshi,sep=""))
  ##-第一部分：得到车的品牌及车系-##
  #临时车名
  input_test1<-input_cxk %>% dplyr::select(car_id,brand_name,series_name,model_name)
  input_test1$car_id<-as.integer(input_test1$car_id)
  rm_series_rule$qx_series_all<-as.character(rm_series_rule$qx_series_all)
  rm_series_rule$series<-as.character(rm_series_rule$series)
  input_test1$model_name<-gsub(" ","",input_test1$model_name)
  input_test1$model_name<-gsub("2012年|-2011 款","",input_test1$model_name)
  input_test1$model_name<-gsub(c(str_c(c(1990:2030,90:99),"款",sep="",collapse='|')),"",input_test1$model_name)
  input_test1$model_name<-fun_normalization(input_test1$model_name)
  #input_test2<-input_test1    input_test1<-input_test2
  #--前期准备：提取准确的brand和series--
  brand_name<-str_extract(input_test1$model_name,c(str_c(rm_series_rule$rule_name,sep="",collapse = "|")))
  brand_name[which(is.na(brand_name))]<-""
  linshi_series<-c(str_c(rm_series_rule$rule_series,sep="",collapse = "|"))
  series_name<-str_extract(input_test1$model_name,gsub(" ","",linshi_series))
  series_name[which(is.na(series_name))]<-""
  linshi<-str_extract(input_test1$model_name,"A佳|N3佳|-进口|进口")
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
    dplyr::select(car_id,brand_name,series_name,model_name,id,name,series,qx_series_all)
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
    dplyr::select(car_id,brand_name,series_name,model_name,id,name,series,qx_series_all)
  #--第三步：model_name匹配系列（存在别克赛欧-雪佛兰赛欧等）--
  a3<-data.frame(car_id=as.integer(setdiff(input_test1$car_id,a2$car_id)))
  input_test1<-inner_join(input_test1,a3,by="car_id")%>%dplyr::select(-qx_series_all)
  car_name_info<-str_extract(paste(input_test1$series_name,input_test1$model_name,sep = ""),gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|"))))
  a3<-data.frame(input_test1[grep("",car_name_info),],series_t=car_name_info[grep("",car_name_info)])
  #全称匹配xilie到车300gsub(" ","",c(str_c(rm_series_rule$series,sep="",collapse = "|")))
  a3$series_t<-as.character(a3$series_t)
  rm_series_rule$series_t<-as.character(rm_series_rule$series_t)
  a3<-inner_join(a3,rm_series_rule,c("series_t"="series_t"))%>%
    dplyr::select(car_id,brand_name,series_name,model_name,id,name,series,qx_series_all)
  #--第四步：未匹配上a4--#
  a4<-data.frame(car_id=setdiff(input_test1$car_id,a3$car_id))
  if(nrow(a4)==0){
    return_data_input<-rbind(a1,a2,a3)
  }else{
    a4<-data.frame(inner_join(a4,input_test1,c("car_id"="car_id")),id="",name="",series="",qx_series_all="")
    a4$name<-a4$brand_name
    a4$series<-a4$series_name
    return_data_input<-rbind(a1,a2,a3,a4)
  }
  
  ########----组合所有car_id---###########
  return_data_input<-inner_join(return_data_input,input_cxk,by="car_id")%>%
    dplyr::select(car_id,brand_name=name,series_name=series,model_name=model_name_t,model_price,model_year,qx_series_all,auto=car_auto,liter,discharge_standard)
  return_data_input$model_name<-toupper(return_data_input$model_name)
  return_data_input$auto<-gsub("-／","",return_data_input$auto)
  linshi<-str_extract(paste0(return_data_input$series_name,return_data_input$model_name),"进口")
  linshi[which(is.na(linshi))]<-""
  linshi<-gsub("进口","-进口",linshi)
  return_data_input$series_name<-paste(return_data_input$series_name,linshi,sep = "")
  #---准确性accurate
  accurate<-c(nrow(a4),nrow(input_cxk))
  return(return_data_input)
}
#基础函数（处理手自动）
dealFun_auto<-function(input_auto){
  input_auto<-gsub("CVT无级变速|无级变速|自动.*CVT|CVT.*自动|-CVT|CVT","自动",input_auto)
  input_auto<-gsub("双离合.*手自动一体","自动",input_auto)
  input_auto<-gsub("手自一体型|手自动一体型|手自动一体|手自一体","自动",input_auto)
  input_auto<-gsub("DSG双离合|双离合器|双离合|G-DCT|DCT|DSG","自动双离合",input_auto)
  input_auto<-gsub("[^a-zA-Z0-9]AT|(-| )[0-9]AT|AT( |-)","自动",input_auto)
  input_auto[grep("AT[^a-zA-Z0-9]",input_auto)]<-gsub("AT","自动",input_auto[grep("AT[^a-zA-Z0-9]",input_auto)])
  input_auto<-gsub("AMT智能手动版|AMT智能手动|AMT|EMT|IMT"," 自动 ",input_auto)
  #手动
  input_auto<-gsub("^MT|[^a-zA-Z0-9]MT|MT(-| )|[0-9]MT","手动",input_auto)
  #car_auto获取手自动
  input_auto<-str_extract(input_auto,"自动|手动")
  return(input_auto)
}
#函数（处理排放标准）
dealFun_discharge_standard<-function(input_model_name,input_discharge_standard){
  car_OBD<-str_extract(input_model_name,"OBD")
  car_OBD[which(is.na(car_OBD))]<-""
  car_discharge_standard<-str_extract(input_model_name,"(国|欧)(Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(型|)|京(5|五|V)")
  input_model_name<-gsub("(国|欧)(Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(型|)|京(5|五|V)","",input_model_name)
  ##填写的排量标准----------------------------------------------------------------------------------------------------
  car_discharge<-input_discharge_standard
  if(length(grep('国|欧|京',car_discharge))==0){
    car_discharge<-car_discharge_standard
  }else{if(length(grep('国|欧|京',car_discharge))!=length(car_discharge)){car_discharge[-grep('国|欧|京',car_discharge)]<-car_discharge_standard[-grep('国|欧|京',car_discharge)]}}
  if(length(grep('国|欧|京',car_discharge))!=length(car_discharge)){car_discharge[-grep('国|欧|京',car_discharge)]<-NA}
  car_discharge<-gsub("1","一",car_discharge)
  car_discharge<-gsub("3|III|Ⅲ","三",car_discharge)
  car_discharge<-gsub("2|Ⅱ|II","二",car_discharge)
  car_discharge<-gsub("4|IV|Ⅳ","四",car_discharge)
  car_discharge<-gsub("6|VI|Ⅵ","六",car_discharge)
  car_discharge<-gsub("5|V","五",car_discharge)
  car_discharge<-gsub("\\+OBD|\\)","",car_discharge)
  car_discharge<-gsub("\\(","\\/",car_discharge)
  return(list(return_qx_name=input_model_name,car_discharge=car_discharge,car_OBD=car_OBD))
}

##********************停用词处理：用于清洗名称中不规范词汇（统一命名规则**********************##
fun_stopWords<-function(data_input){
  car_model_name<-toupper(data_input$model_name)
  car_price<-data_input$model_price
  car_name<-toupper(data_input$brand_name)
  car_series1<-toupper(data_input$series_name)
  car_discharge_standard<-data_input$discharge_standard
  qx_name<-toupper(data_input$model_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  qx_name<-gsub("2012年","2012款",qx_name)
  qx_name<-gsub("-2011 款","2011款 ",qx_name)
  car_year<-str_extract(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  car_year<-gsub("款","",car_year)
  car_year[which(nchar(car_year)==2)]<-paste("20",car_year[which(nchar(car_year)==2)],sep = "")
  loc_year<-str_locate(qx_name,"[1-2][0-9][0-9][0-9]款|[0-9][0-9]款|2000")
  qx_name<-str_sub(qx_name,loc_year[,2]+1)
  qx_name<-gsub(c(str_c(1990:2030,"款 ",sep="",collapse='|')),"",qx_name)
  if(length(grep('20',data_input$model_year))!=0){car_year=data_input$model_year}
  qx_name<-gsub("\\（","(",qx_name)
  qx_name<-gsub("\\）",")",qx_name)
  qx_name<-gsub("\\(进口\\)","",qx_name)
  qx_name<-gsub("\\(海外\\)","",qx_name)
  qx_name<-gsub("欧尚COS1°\\(科赛1°\\)|欧尚COS1°|欧尚科赛1°","欧尚科赛",qx_name)
  qx_name<-gsub("欧尚COSMOS\\(科尚\\)|欧尚COSMOS","欧尚科尚",qx_name)
  qx_name<-gsub("欧尚EV A600 EV|欧尚EVA600EV|欧尚EV A600|欧尚EV","欧尚A600EV",qx_name)
  qx_name<-gsub("POWER DAILY","宝迪",qx_name)
  qx_name<-gsub("III","Ⅲ",qx_name)
  qx_name<-gsub("II","Ⅱ",qx_name)
  qx_name<-gsub("—|－","-",qx_name)
  qx_name<-gsub("\\·|\\?|(-|)JL465Q","",qx_name)
  qx_name<-gsub("ONE\\+","ONE佳",qx_name)
  qx_name<-gsub("劲能版\\+","劲能版佳",qx_name)
  qx_name<-gsub("选装(包|)","佳",qx_name)
  qx_name<-gsub("格锐","格越",qx_name)
  qx_name<-gsub("2\\/3|2\\+2\\+3|2\\+3\\+2","",qx_name)
  qx_name<-fun_normalization(qx_name)

  car_series1<-gsub("格锐","格越",car_series1)
  car_series1<-gsub("\\（","(",car_series1)
  car_series1<-gsub("\\）",")",car_series1)
  car_series1<-gsub("\\(进口\\)|\\(海外\\)","",car_series1)
  forFun<-function(i){
    sub(car_series1[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  forFun<-function(i){
    sub(car_name[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_name),forFun))
  qx_name[grep("\\+",car_series1)]<-sub("\\+","",qx_name[grep("\\+",car_series1)])
  if('series_group_name'%in%names(data_input)==T){
    car_series1[grep('进口',data_input$series_group_name)]<-paste0(car_series1[grep('进口',data_input$series_group_name)],'-进口')}
  car_series1<-gsub("-$","",car_series1)
  car_series1<-gsub('进口-进口','进口',car_series1)
  ##--series停用词----
  car_series1<-gsub("全新奔腾","奔腾",car_series1)
  car_series1<-gsub("北京汽车|北京","",car_series1)
  car_series1<-gsub("^JEEP$","北京JEEP",car_series1)
  car_series1<-gsub("依维柯Venice|Venice","威尼斯",car_series1)
  car_series1<-gsub("锋范经典","锋范",car_series1)
  car_series1<-gsub("名爵ZS","MGZS",car_series1)
  car_series1<-gsub("MINI |SMART ","",car_series1)
  car_series1<-gsub("PASSAT","帕萨特",car_series1)
  car_series1<-gsub("塞纳SIENNA|SIENNA","塞纳",car_series1)
  car_series1<-gsub("\\+","佳",car_series1)
  
  ###--------清洗奔驰------
  car_series1<-gsub("GRAND EDITION","特别版",car_series1)
  car_series1<-gsub("改装房车","",car_series1)
  qx_name[grep("奔驰",car_series1)]<-gsub("AMG","",qx_name[grep("奔驰",car_series1)])
  benchi<-str_extract(car_series1,"荣威.*|福特(E|F).*|捷豹.*|奥迪TTS|奥迪TT|奥迪A8|奥迪S[1-9]|奔驰.*")
  benchi<-gsub("荣威|福特E|福特|捷豹|奥迪|奔驰|(级| |-).*","",benchi)
  benchi[-grep("",benchi)]<-""
  #清除e L等
  forFun<-function(i){
    sub(benchi[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  ##获取奔驰数字串
  linshi<-str_extract(car_series1,"奔驰.*")
  linshi[grep("奔驰",car_series1)]<-str_extract(qx_name[grep("奔驰",car_series1)],"[0-9]{3}|[0-9]{2}")
  linshi[which(is.na(linshi))]<-""
  linshi[-grep("",linshi)]<-""
  benchi<-paste(benchi,linshi,sep = "")
  #清除数字
  forFun<-function(i){
    sub(linshi[i],"",qx_name[i])
  }
  qx_name<-unlist(lapply(1:length(car_series1),forFun))
  car_series1[grep("奔驰",car_series1)]<-gsub("级","",car_series1[grep("奔驰",car_series1)])
  
  ##清洗规则
  qx_name<-gsub("型|舱|款|版","型",qx_name)
  qx_name<-gsub("FR\\+","FR佳",qx_name)
  qx_name<-gsub("FRESH(-|\\+)ALLOY","天地",qx_name)
  qx_name<-gsub("世博型|世博","贺岁",qx_name)
  qx_name<-gsub("个性","限量",qx_name)
  qx_name<-gsub("新白金","白金",qx_name)
  qx_name<-gsub("超值物流","傲运通",qx_name)
  qx_name<-gsub("(一|二|两|三|四|五|十|[0-9]+)周年","周年",qx_name)
  qx_name<-gsub("周年纪念","纪念",qx_name)
  qx_name<-gsub("不带OBD|毂|16.4|5阀|带PEP|无侧拉门|酷黑骑士|油改气|单增压|节能|系列","",qx_name)
  qx_name<-gsub("带空调|空调型","空调",qx_name)
  qx_name<-gsub("Féline","Feline",qx_name)
  qx_name<-gsub("M/T","MT",qx_name)
  qx_name<-gsub("A/T","AT",qx_name)
  qx_name<-gsub("4驱|AWD|4WD-4|4WD|4X4|4×4|4\\*4","四驱",qx_name)
  qx_name<-gsub("2WD|4X2|4×2|4\\*2","两驱",qx_name)
  qx_name<-gsub("1.8T/5V","1.8T",qx_name)
  qx_name<-gsub("HYBRID.*(新|)混合动力|(新|)混合动力.*HYBRID|HYBRID|混合动力","混动",qx_name)
  qx_name<-gsub("MHD","微型混动",qx_name)
  qx_name<-gsub("钛金内饰版|象牙内饰版","型",qx_name)
  qx_name<-gsub("马力|公里","KW",qx_name)
  qx_name<-gsub("新一代|(第|)(Ⅰ|一|I|1)(代|型)","一代",qx_name)
  qx_name<-gsub("(第|)(Ⅱ|二|2)(代|型)","二代",qx_name)
  qx_name<-gsub("(第|)(Ⅲ|三|3)(代|型)","三代",qx_name)
  qx_name<-gsub("(第|)(Ⅳ|四|4)(代|型)","四代",qx_name)
  qx_name<-gsub("(第|)(五|5)(代|型)","五代",qx_name)
  qx_name<-gsub("LSTD","加长型 标准型",qx_name)
  qx_name<-gsub("GSTD","加高型 标准型",qx_name)
  qx_name<-gsub("LDLX","加长型 豪华型",qx_name)
  qx_name<-gsub("GDLX","加高型 豪华型",qx_name)
  qx_name[grep("STD.*标准|标准.*STD",qx_name)]<-gsub("STD1|STD2|STD","",qx_name[grep("STD.*标准|标准.*STD",qx_name)])
  qx_name[grep("^MG6",car_series1)]<-gsub("两厢","掀背",qx_name[grep("^MG6",car_series1)])
  car_series1[grep("索纳塔",car_series1)][grep("2011",car_year[grep("索纳塔",car_series1)])]<-"索纳塔八"
  qx_name<-gsub("STD(\\(.*\\)|X|)","标准型",qx_name)
  qx_name<-gsub("DLX(\\(.*\\)|)","豪华型",qx_name)
  qx_name<-gsub("(硬|软)顶(型|)","",qx_name)
  qx_name<-gsub("(敞篷|敞蓬)(型|车|)","敞篷",qx_name)
  qx_name<-gsub("商用车","商务车",qx_name)
  qx_name<-gsub("型.*爵士黑","爵士型",qx_name)
  qx_name<-gsub("(双门|)轿跑(车|型|)","COUPE",qx_name)
  qx_name<-gsub("(双门|)跑车(型|)","COUPE",qx_name)
  qx_name<-gsub("柴油VE泵"," VE泵柴油",qx_name)
  qx_name<-gsub("柴油共轨","共轨柴油",qx_name)
  qx_name<-gsub("\\+助力转向|助力转向|(转向|加装|有|带| )助力","助力",qx_name)
  qx_name<-gsub("双燃料(型|车)","双燃料",qx_name)
  qx_name<-gsub("单燃料(型|车)","单燃料",qx_name)
  qx_name<-gsub("液化石油气","LPG",qx_name)
  qx_name<-gsub("压缩天然气","CNG",qx_name)
  qx_name<-gsub("液化天然气","LNG",qx_name)
  qx_name<-gsub("厢货车|厢式货车","厢货",qx_name)
  qx_name<-gsub("箱","厢",qx_name)
  qx_name<-gsub("变速厢","变速箱",qx_name)
  qx_name<-gsub("厢式车|厢车","厢式",qx_name)
  qx_name<-gsub("货厢","厢",qx_name)
  qx_name<-gsub("标准厢","标厢",qx_name)
  qx_name<-gsub("仓栏车|仓栅车","仓栅",qx_name)
  qx_name<-gsub("加长车|加长型","加长",qx_name)
  qx_name<-gsub("长车身|长型","长车",qx_name)
  qx_name<-gsub("短车身|短型","短车",qx_name)
  qx_name<-gsub("天窗型车身|(全景|带|新)天窗(型|)","天窗",qx_name)
  qx_name<-gsub("真皮座椅|真皮型","真皮",qx_name)
  qx_name<-gsub("织物座椅|织物型","织物",qx_name)
  qx_name<-gsub("带.*GO.*功能","",qx_name)
  qx_name<-gsub("铝合金轮","铝轮",qx_name)
  qx_name<-gsub("BLACKORANGE","墨橘",qx_name)
  qx_name<-gsub("精典","经典",qx_name)
  qx_name<-gsub("经济配置","经济",qx_name)
  qx_name<-gsub("经典特别","特别",qx_name)
  qx_name<-gsub("物流型","物流车",qx_name)
  qx_name<-gsub("欧风型|欧洲型|欧规型|欧规|欧风|欧型","欧洲",qx_name)
  qx_name[grep("蓝瑟|卡罗拉|风云2",car_series1)]<-gsub("掀背|特装|卓越","",qx_name[grep("蓝瑟|卡罗拉|风云2",car_series1)])
  qx_name[grep("名图",car_series1)]<-gsub("豪华","",qx_name[grep("名图",car_series1)])
  qx_name<-gsub("F-150","F150",qx_name)
  qx_name<-gsub("F-350","F350",qx_name)
  qx_name<-gsub("F-450","F550",qx_name)
  qx_name<-gsub("蛇行","蛇形",qx_name)
  
  #######-----------正则排量标准-------
  return_dealFun_discharge_standard<-dealFun_discharge_standard(qx_name,data_input$discharge_standard)
  car_discharge<-return_dealFun_discharge_standard$car_discharge
  car_OBD<-return_dealFun_discharge_standard$car_OBD
  qx_name<-return_dealFun_discharge_standard$return_qx_name
  
  ###------------改款等---------#########
  qx_name<-gsub("(独立|侧翻)座椅|带侧拉门|新型|改型(经典|双擎|E|)|双擎|\\(\\+\\)|冠军型|重装型|(08)升级型|换代|改装型|PLUS(型|)|[0-9]系|通用GMC|公爵|御系列","改版",qx_name)
  car_restyle<-trim(str_extract(qx_name,"升级型|改版"))
  linshi<-str_extract(qx_name,"(一|二|三|四|五)代")
  linshi[which(is.na(linshi))]<-""
  car_restyle[which(is.na(car_restyle))]<-""
  car_restyle<-paste(car_restyle,linshi,sep="")
  qx_name<-gsub("\\(北京型\\)|升级型|改版|(一|二|三|四|五)代","",qx_name)
  
  ####--------------高中平低顶----------
  car_height_t<-str_extract(qx_name,"(半高|标准|高|中低|超低|中|平|低)顶|([0-9][.][0-9]|[0-9]+)米|[0-9]{4}长")
  qx_name<-gsub("(半高|标准|高|中低|超低|中|平|低)顶|([0-9][.][0-9]|[0-9]+)米|[0-9]{4}长","",qx_name)
  
  ######################-----------人人车中轴距没有数据------------------------------########
  #轴距
  car_wheelbase<-str_extract(qx_name,"轴距[0-9]{4}|[0-9]{4}轴距|加长轴加长轴|(加长|标准|中短|超长|中短|长|中|短)轴|轴距")
  qx_name<-gsub("轴距[0-9]{4}|[0-9]{4}轴距|加长轴加长轴|(加长|标准|中短|超长|中短|长|中|短)轴|轴距","",qx_name)
  
  ####------------------燃料类型---------------
  car_oil<-str_extract(qx_name,"(三菱|丰田4Y长城)发动机|(丰田4Y|)绵阳|全柴|汽油|(2|莱动|VE泵|高压共轨|共轨|)柴油|混动|微型混动|双燃料|单燃料")
  linshi<-str_extract(qx_name,"LPG|LNG|CNG")
  linshi[which(is.na(linshi))]<-""
  car_oil[which(is.na(car_oil))]<-""
  car_oil<-paste(car_oil,linshi,sep="")
  qx_name<-gsub("混动(型|车|)|(493|491|2| |莱动|VE泵|高压共轨|共轨|)柴油(车型|发动机|型|机|)|汽油(车型|发动机|型|机|)|(微型|E驱)混动|(LPG|LNG|CNG)(车型|)|双燃料|491发动机|全柴|(丰田4Y|)(绵阳|长城)(发动机|)|三菱发动机"," ",qx_name)
  #----是否原装及进口-----
  linshi<-str_extract(qx_name,"节油Π|节油|平行进口|国机|K机|(组装|原装|进口)")
  linshi[which(is.na(linshi))]<-""
  car_oil<-paste(car_oil,linshi,sep="")
  qx_name<-gsub("智能节油|节油Π|节油|平行进口|国机|K机|(组装|原装|进口)(发动机|机|)"," ",qx_name)
  
  ####--------车座位--------------真皮-丝绒--天窗---
  qx_name<-gsub("人座","座",qx_name)
  qx_name<-gsub("七座","7座",qx_name)
  qx_name<-gsub("六座","6座",qx_name)
  qx_name<-gsub("五座","5座",qx_name)
  qx_name<-gsub("两座","2座",qx_name)
  car_site<-str_extract(qx_name,"([0-9]+-|-|)[0-9]+(\\/[0-9]+\\/[0-9]+|\\/[0-9]+|)座|9\\+座|(2|4|5|7)人")
  qx_name<-gsub("([0-9]+-|-|)[0-9]+(\\/[0-9]+\\/[0-9]+|\\/[0-9]+|)(座型|座)|9\\+(座型|座)|(2|4|5|7)人","",qx_name)
  car_site<-gsub("^-|\\+","",car_site)
  car_site<-gsub("-","/",car_site)
  car_site<-gsub("人","座",car_site)
  ##真皮-丝绒
  linshi<-str_extract(qx_name,"真皮|丝绒|织物")
  linshi[grep("",linshi)]<-linshi[grep("",linshi)]
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  #天窗
  linshi<-str_extract(qx_name,"天窗经典|双天窗|天窗")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  #空调
  linshi<-str_extract(qx_name,"空调")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  #纪念
  linshi<-str_extract(qx_name,"纪念")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_site[which(is.na(car_site))]<-""
  car_site<-paste(car_site,linshi,sep="")
  qx_name<-gsub("真皮|丝绒|织物|天窗经典|双天窗|天窗|空调|纪念"," ",qx_name)
  
  #######----------car_series3提炼车型-------
  car_series3<-str_extract(qx_name,c(str_c(unique(rm_rule$qx_series3),sep="",collapse = "|")))
  qx_name<-gsub(c(str_c(unique(rm_rule$qx_series3),sep="",collapse = "|")),"",qx_name)
  linshi<-str_extract(qx_name,"掀背")
  linshi[which(is.na(linshi))]<-""
  car_series3[which(is.na(car_series3))]<-""
  car_series3<-paste(car_series3,linshi,sep="")
  
  #NAVI和导航型清洗
  qx_name[grep("(导航(型|).*)NAVI|(NAVI(.*导航(型|)))",qx_name)]<-
    gsub("导航(型|)","",qx_name[grep("(导航(型|).*)NAVI|(NAVI(.*导航(型|)))",qx_name)])
  car_series3[grep("(-|)NAVI(型|-|)",qx_name)]<-"导航型"
  qx_name[grep("(-|)NAVI(型|-|)",qx_name)]<-gsub("(-|)NAVI(型|-|)"," ",qx_name[grep("(-|)NAVI(型|-|)",qx_name)])
  linshi<-str_extract(qx_name,"(特睿|影音|音乐|电子|语音|北斗|)导航(爵士|ESP|)")
  linshi[grep("",linshi)]<-str_c(linshi[grep("",linshi)],"型",sep="")
  linshi[which(is.na(linshi))]<-""
  car_series3[which(is.na(car_series3))]<-""
  car_series3<-paste(car_series3,linshi,sep="")
  qx_name<-gsub("(特睿|影音|音乐|电子|(DVD|)语音|北斗|)导航(爵士|ESP|)(型|)|掀背(型|)"," ",qx_name)
  
  #马力（KW）car_hp
  car_hp<-str_extract(qx_name,"(([0-9]|)[0-9][.][0-9]|[0-9]+)KW|大KW(型|)")
  qx_name<-gsub("(([0-9]|)[0-9][.][0-9]|[0-9]+)KW|大KW(型|)","",qx_name)
  
  ##清除排量-国标car_displacement_standard--car_Displacement---car_Displacement_t(排量类型)---car_oil(燃油类型)---car_drive(几驱)
  ###---排量car_lter_t还需要清洗（命名规则统一）|T|L|i
  car_desc<-str_extract(qx_name,"[0-9][.][0-9](| )(TFSI|FSI|TSI|CRDI|TDDI)|[0-9][.][0-9](GTDI|(| )TDI|TGI|XV|GX|GV|GS|THP|GDIT|TGDI|GDI|TCI|TID|GT|DIT|XT|TD|GI|HQ|HG|HV|TI|TC|VTEC|VVT-I|DCVVT|CVVT|DVVT|VVT)|[0-9]+(GDIT|TGDI|TSI|THP|HP)|(TSI|THP|HP|GTDI)[0-9][0-9][0-9]|(GDIT|GTDI|GDI|TFSI|FSI|TSI|SIDI|TCI |VTEC|VVT-I|DCVVT|CVVT|DVVT|VVT|MPI)|([0-9][0-9]( |)|)(TFSI|FSI|TDI)| ((18|20|25|28|40|50)T|30E|(15|25|36)S|15N|30H) ")
  #别克.*(XT|GT|LT|CT3|CT2|CT1|CT)  car_desc
  linshi<-str_extract(qx_name[grep("GL8|凯越",car_model_name)],"LE|LX|LS|XT|GT|LT|CT3|CT2|CT1|CT")
  linshi[which(is.na(linshi))]<-""
  car_desc[which(is.na(car_desc))]<-""
  car_desc[grep("GL8|凯越",car_model_name)]<-paste(car_desc[grep("GL8|凯越",car_model_name)],linshi,sep = "")
  qx_name[grep("GL8|凯越",car_model_name)]<-
    gsub("LE|LX|LS|XT|GT|LT|CT3|CT2|CT1|CT","",qx_name[grep("GL8|凯越",car_model_name)])
  car_liter_wu<-str_extract(qx_name,"[0-9][.][0-9](T|D|JS|JE|JC|J|SE|SL|SX|SI|SC|S|H|G|E|V|L|I|)")
  car_liter_wu[which(is.na(car_liter_wu))]<-""
  qx_name<-gsub("[0-9][.][0-9](| )(TFSI|FSI|TSI|CRDI|TDDI)|[0-9][.][0-9](GTDI|(| )TDI|TGI|XV|GX|GV|GS|TSI|THP|GDIT|TGDI|GDI|TCI|TID|GT|DIT|XT|TD|GI|HQ|HG|HV|TI|TC|VTEC|VVT-I|DCVVT|CVVT|DVVT|VVT|T|D|SE|SL|SX|SI|SC|S|H|G|E|V|L|I)|[0-9]+(GDIT|TGDI|TSI|THP|HP)|(TSI|THP|HP|GTDI)[0-9][0-9][0-9]|(GDIT|GTDI|GDI|TFSI|FSI|TSI|SIDI|TCI |VTEC|VVT-I|DCVVT|CVVT|DVVT|VVT|MPI)|([0-9][0-9]( |)|)(TFSI|FSI|TDI)| ((18|20|25|28|40|50)T|30E|(15|25|36)S|15N|30H) |[0-9][.][0-9]"," ",qx_name)
  #提炼VTI-   car_desc
  car_desc[grep("TYPE-S|4MATIC|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I",qx_name)]<-
    str_extract(qx_name,"TYPE-S|4MATIC|GLXI|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I")[grep("TYPE-S|4MATIC|GLXI|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I",qx_name)]
  qx_name<-gsub("TYPE-S|4MATIC|GLXI|GLXI|VTI-L|VTI-E|VTI-S|LXI|EXI|VTI[0-9]+|VTI|GLX-I|GS-I|GL-I|G-I","",qx_name)
  ####对倒置的数据进行归一化
  car_desc<-gsub(" ","",car_desc)
  car_liter_wu[grep("[0-9][.][0-9].*",car_desc)]<-car_desc[grep("[0-9][.][0-9].*",car_desc)]
  car_desc<-gsub("[0-9][.][0-9].*| ","",car_desc)
  car_desc[which(is.na(car_desc))]<-""
  ce1<-str_extract(car_desc,"[0-9]+")
  ce1[which(is.na(ce1))]<-""
  car_desc<-gsub("[0-9]+","",car_desc)
  car_desc<-paste(ce1,car_desc,sep = "")
  
  ######-------发动机类型-----
  car_engine<-str_extract(qx_name,c(str_c(rm_rule$qx_engine,"(([A-Za-z0-9]|-)+|)",sep="",collapse = "|")))
  qx_name<-gsub(paste(rm_rule$qx_engine,"(([A-Za-z0-9]|-)+|)",sep="",collapse = "|"),"",qx_name)
  car_engine1<-str_extract(qx_name,c(str_c("H8D|HAD|H6B|H5B|H3B|GZ[0-9]{3}|B1A|B3BZ|B3XZ|B6B|B6BZ|BAD","(([A-Za-z0-9]|-)+|)",sep="",collapse = "|")))
  qx_name<-gsub(paste("H8D|HAD|H6B|H5B|H3B|GZ[0-9]{3}|B1A|B3BZ|B3XZ|B6B|B6BZ|BAD","(([A-Za-z0-9]|-)+|)",sep="",collapse = "|"),"",qx_name)
  
  ##汽车驱动类型
  qx_name<-gsub("前驱|后驱(型|)","两驱",qx_name)
  car_drive<-str_extract(qx_name,"两驱|四驱|全驱|(无|)助力")
  qx_name<-gsub("带OBD|OBD系列|\\+OBD|OBD"," ",qx_name)
  qx_name<-gsub("(全驱|两驱|(分时|全时|)四驱)(型|)|(有|无|)助力"," ",qx_name)
  
  ##----手自动清洗----(CVT|DSG|AT|MT|AMT|G-DCT|DCT)
  qx_name<-gsub("CVT无级变速|无级变速|自动.*CVT|CVT.*自动|-CVT|CVT","自动",qx_name)
  qx_name<-gsub("双离合.*手自动一体","自动",qx_name)
  qx_name<-gsub("手自一体型|手自动一体型|手自动一体|手自一体","自动",qx_name)
  qx_name<-gsub("DSG双离合|双离合器|双离合|G-DCT|DCT|DSG","自动双离合",qx_name)
  qx_name<-gsub("[^a-zA-Z0-9]AT|(-| )[0-9]AT|AT( |-)","自动",qx_name)
  qx_name[grep("AT[^a-zA-Z0-9]",qx_name)]<-gsub("AT","自动",qx_name[grep("AT[^a-zA-Z0-9]",qx_name)])
  qx_name<-gsub("AMT智能手动版|AMT智能手动|AMT|EMT|IMT"," 自动 ",qx_name)
  #手动
  qx_name<-gsub("^MT|[^a-zA-Z0-9]MT|MT(-| )","手动",qx_name)
  ##car_auto获取手自动
  car_auto<-data_input$auto
  car_auto<-str_extract(qx_name,"自动|G-DCT|[0-9]MT|手动")
  qx_name<-gsub("(自动|手动)(型|挡|档|变速)|自动|手动|[0-9](档|挡)|(4|5|6|六)速|[^a-zA-Z0-9]MT|(-| )[0-9]MT","",qx_name)
  
  #双离合
  linshi<-str_extract(qx_name,"双离合")
  linshi[which(is.na(linshi))]<-""
  car_hp[which(is.na(car_hp))]<-""
  car_hp<-paste(car_hp,linshi,sep="")
  qx_name<-gsub("双离合","",qx_name)
  
  ##car_door几门几款-------
  qx_name<-gsub("双门|二门","两门",qx_name)
  qx_name<-gsub("大双排","大双",qx_name)
  qx_name<-gsub("小双排","小双",qx_name)
  qx_name<-gsub("中双排","中双",qx_name)
  qx_name<-gsub("标双排","标双",qx_name)
  qx_name<-gsub("一排半","排半",qx_name)
  qx_name<-gsub("大单排","大单",qx_name)
  qx_name<-gsub("3门","三门",qx_name)
  qx_name<-gsub("4门","四门",qx_name)
  qx_name<-gsub("5门","五门",qx_name)
  car_door<-str_extract(qx_name,"两门|三门|四门|五门|(大|小|中)(双|单)|标双|(双|单)排|CROSS($| )|QUATTRO|排半")
  qx_name<-gsub("(两门|三门|四门|五门)(型|)|(大|小|中)(双|单)|标双|(铁|)(双|单)排|CROSS($| )|QUATTRO|排半|云100"," ",qx_name)
  
  # ########%$^^^%%^%^清洗垃圾信息%……￥…………#######
  # qx_name[-grep("型",qx_name)]<-paste0(qx_name[-grep("型",qx_name)],"型",sep="")
  # linshi<-str_locate_all(qx_name,"型")
  # linshi_wz<-sapply(linshi,length)
  # wzfun<-function(i){
  #   linshi[[i]][linshi_wz[i]]
  # }
  # wz<-unlist(lapply(1:length(linshi), wzfun))
  # qx_name<-str_sub(qx_name,1,wz)
  # ##----####
  # qx_name<-gsub("风行|XRV|广汽|哈弗M4|吉利EC7|吉利|吉普|JEEP|劲取|卡宴|旗云|日产|长安EADO|长安|长丰|制造|致胜","",qx_name)
  # qx_name<-gsub(c(str_c(rm_che58$key,".*",sep="",collapse = "|")),"",qx_name)
  # ########%$^^^%%^%^清洗垃圾信息%……￥…………#######
  
  #####------qx_name最后清洗----------
  qx_name<-gsub("\\/|--","-",qx_name)
  qx_name<-gsub(" -|- |^-|-$|\\(|\\)|型"," ",qx_name)
  qx_name<-trim(qx_name)
  qx_name<-gsub("转向"," ",qx_name)
  qx_name<-gsub("LUXURY","LUX",qx_name)
  ################qx_name<-gsub("XL-LUX|LUX","至尊",qx_name)
  qx_name<-gsub(" +"," ",qx_name)
  
  ############----------------测试qx_name----------
  qx_name1<-str_extract(qx_name,"(([\u4e00-\u9fa5]+ |)[\u4e00-\u9fa5]+ [\u4e00-\u9fa5]+)|[\u4e00-\u9fa5]+([A-Za-z0-9]+|)[\u4e00-\u9fa5]+|[\u4e00-\u9fa5]+|[\u4e00-\u9fa5]")
  qx_name1[which(is.na(qx_name1))]<-""
  forFun1<-function(i){
    sub(qx_name1[i],"",qx_name[i])
  }
  qx_name2<-unlist(lapply(1:length(qx_name1),forFun1))
  linshi<-str_extract(qx_name2,"[\u4e00-\u9fa5]+ [\u4e00-\u9fa5]+|[\u4e00-\u9fa5]+|[\u4e00-\u9fa5]")
  linshi[which(is.na(linshi))]<-""
  forFun<-function(i){
    sub(linshi[i],"",qx_name2[i])
  }
  qx_name2<-unlist(lapply(1:length(linshi),forFun))
  qx_name2<-gsub("\\/|--|\\+","-",qx_name2)
  qx_name2<-gsub(" -|- |^-|-$|GPS|\\(|\\)|型"," ",qx_name2)
  qx_name2<-trim(qx_name2)
  qx_name2<-gsub(" +"," ",qx_name2)
  qx_name1<-gsub(" ","",paste(qx_name1,linshi,sep=""))
  #######qx_name1顺序整理##########
  linshi<-str_extract(qx_name1,"炫装|荣耀|足金")
  qx_name1<-gsub("炫装|荣耀|足金","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"运动")
  qx_name1<-gsub("运动","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"白金|劲悦|罗兰加洛斯|周年|典藏")
  qx_name1<-gsub("白金|劲悦|罗兰加洛斯|周年|典藏","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"智慧")
  qx_name1<-gsub("智慧","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"经典")
  qx_name1<-gsub("经典","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"阳光")
  qx_name1<-gsub("阳光","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"领航")
  qx_name1<-gsub("领航","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  linshi<-str_extract(qx_name1,"精锐|精英")
  qx_name1<-gsub("精锐|精英","",qx_name1)
  linshi[which(is.na(linshi))]<-""
  qx_name1[which(is.na(qx_name1))]<-""
  qx_name1<-paste(qx_name1,linshi,sep="")
  ##---对qx_name1清洗-------###
  qx_name1<-gsub("标准酷行","酷行",qx_name1)
  qx_name1<-gsub("豪华酷翔","酷翔",qx_name1)
  qx_name1<-gsub("舒适酷跃","酷跃",qx_name1)
  qx_name1<-gsub("新鸿达|鸿达|已备案|浩纳|索兰托|途安","",qx_name1)
  qx_name1<-gsub("精质","精致",qx_name1)
  qx_name1<-gsub("炫动套装","炫动佳",qx_name1)
  qx_name1<-gsub("智尚","致尚",qx_name1)
  qx_name1[grep("森雅S80",car_series1)]<-gsub("都市","",qx_name1[grep("森雅S80",car_series1)])
  qx_name1[grep("夏利",car_series1)]<-gsub("A佳|N3佳|A|N3","",qx_name1[grep("夏利",car_series1)])
  qx_name2<-gsub("PREMIUM","PRM",qx_name2)
  
  ########输出########
  output_data<-data.frame(car_name,car_year,car_restyle,car_series1,car_series3,benchi,car_drive,qx_name1,qx_name2,
                          car_door,car_liter_wu,car_desc,car_auto,car_site,car_oil,car_discharge,car_hp,car_height_t,car_wheelbase,car_engine,car_engine1,car_OBD,car_discharge_standard,car_price,car_model_name)
  return(output_data)
}
##********************归一化处理：用于将品牌/车系命名统一**********************##
fun_normalization<-function(input_test) {
  input_test<-toupper(input_test)
  input_test<-gsub("改款","",input_test)
  input_test<-gsub("POWERDAILY","宝迪",input_test)
  input_test<-gsub("全新","",input_test)
  input_test<-gsub("\\（","(",input_test)
  input_test<-gsub("\\）",")",input_test)
  input_test<-gsub("\\+","佳",input_test)
  input_test<-gsub("名爵","MG",input_test)
  input_test<-gsub("特斯拉","TESLA",input_test)
  input_test<-gsub("域胜","BW",input_test)
  input_test<-gsub("路虎极光","路虎揽胜极光",input_test)
  input_test<-toupper(input_test)
  input_test<-gsub("\\·|\\?","",input_test)
  input_test<-gsub("\\(进口\\)|\\(海外\\)","-进口",input_test)
  input_test<-gsub("北京汽车|北京","北汽",input_test)
  input_test<-gsub("欧尚COS1°\\(科赛1°\\)|欧尚COS1°|欧尚科赛1°","欧尚科赛",input_test)
  input_test<-gsub("欧尚COSMOS\\(科尚\\)|欧尚COSMOS","欧尚科尚",input_test)
  input_test<-gsub("欧尚EVA600EV|欧尚EVA600|欧尚EV","欧尚A600EV",input_test)
  input_test<-gsub("北汽20","BJ20",input_test)
  input_test<-gsub("北汽40","BJ40",input_test)
  input_test<-gsub("北汽80","BJ80",input_test)
  input_test<-gsub("本田INSIGHT","本田音赛特",input_test)
  input_test<-gsub("丰田奔跑者","丰田4RUNNER",input_test)
  input_test<-gsub("凯雷德ESCALADE|ESCALADE","凯雷德",input_test)
  input_test<-gsub("玛莎拉蒂LEVANTE|LEVANTE","玛莎拉蒂LEVANTE",input_test)
  input_test<-gsub("玛莎拉蒂GRANTURISMO|GRANTURISMO","玛莎拉蒂GT",input_test)
  input_test<-gsub("大众MULTIVAN|MULTIVAN","迈特威",input_test)
  input_test<-gsub("海狮HIACE|HIACE海狮|HIACE","海狮",input_test)
  input_test<-gsub("LANNIA蓝鸟|蓝鸟LANNIA|LANNIA","蓝鸟",input_test)
  input_test<-gsub("LANCER蓝瑟|蓝瑟LANCER|LANCER","蓝瑟",input_test)
  input_test<-gsub("OTTIMO致悦|OTTIMO|致悦","致悦",input_test)
  input_test<-gsub("TIIDA骐达|TIIDA|骐达","骐达",input_test)
  input_test<-gsub("ATENZAO","阿特兹",input_test)
  input_test<-gsub("北汽212系列|北汽212|BJ212","北汽212系列",input_test)
  input_test<-gsub("吉利EC8","帝豪EC8",input_test)
  input_test<-gsub("GALLARDO","盖拉多",input_test)
  input_test<-gsub("RAM","公羊",input_test)
  input_test<-gsub("金威","金龙金威",input_test)
  input_test<-gsub("格锐","格越",input_test)
  input_test<-gsub("总裁","玛莎拉蒂总裁",input_test)
  input_test<-gsub("致胜","蒙迪欧-致胜",input_test)
  input_test<-gsub("V6菱仕|菱仕","V6菱仕",input_test)
  input_test<-gsub("V5菱致|菱致","V5菱致",input_test)
  input_test<-gsub("V3菱悦|菱悦","V3菱悦",input_test)
  input_test<-gsub("YARISL致炫|致炫","YARISL致炫",input_test)
  input_test<-gsub("YARISL致享|致享","YARISL致享",input_test)
  input_test<-gsub("PASSAT","帕萨特",input_test)
  input_test<-gsub("帕萨特领驭|领驭","帕萨特领驭",input_test)
  input_test<-gsub("III","Ⅲ",input_test)
  input_test<-gsub("SPRINTER","斯宾特",input_test)
  input_test<-gsub("吉利SC3","英伦SC3",input_test)
  input_test<-gsub("吉利SC5-RV","英伦SC5-RV",input_test)
  input_test<-gsub("吉利SX7","英伦SX7",input_test)
  input_test<-gsub("MUSTANG","野马",input_test)
  input_test<-gsub("TIGUAN","途观",input_test)
  input_test<-gsub("A\\+","A佳",input_test)
  input_test<-gsub("N3\\+","N3佳",input_test)
  input_test<-gsub("北汽(\\(BJ\\)|BJ)","BJ",input_test)
  input_test<-gsub("全新奔腾","奔腾",input_test)
  input_test<-gsub("锋范经典","锋范",input_test)
  input_test<-gsub("格锐","格越",input_test)
  input_test<-gsub("宝来.*","宝来",input_test)
  input_test<-gsub("ATENZA","阿特兹",input_test)
  input_test<-gsub("PICASSO","毕加索",input_test)
  input_test<-gsub("SAAB","萨博",input_test)
  input_test<-gsub("致尚XT","逸动",input_test)
  input_test<-gsub("KUGA","翼虎",input_test)
  input_test<-gsub("锋势","威驰",input_test)
  input_test<-gsub("路霸","陆霸",input_test)
  input_test<-gsub("MAXUS","大通",input_test)
  input_test<-gsub("YETI","野帝",input_test)
  input_test<-gsub("Scirocco尚酷|Scirocco","尚酷",input_test)
  input_test<-gsub("全新胜达|新胜达","胜达",input_test)
  input_test<-gsub("金龙凯歌|凯歌","金龙凯歌",input_test)
  input_test<-gsub("林肯CONTINENTAL|CONTINENTAL","林肯大陆",input_test)
  input_test<-gsub("Axela昂克赛拉|Axela","昂克赛拉",input_test)
  input_test<-gsub("奔驰GLC","奔驰GLC级",input_test)
  input_test<-gsub("奔驰GLA","奔驰GLA级",input_test)
  input_test<-gsub("奔驰GLE","奔驰GLE级",input_test)
  input_test<-gsub("奔驰GLS","奔驰GLS级",input_test)
  input_test<-gsub("奔驰GLK","奔驰GLK级",input_test)
  input_test<-gsub("奔驰SLC","奔驰SLC级",input_test)
  input_test<-gsub("奔驰SLK","奔驰SLK级",input_test)
  input_test<-gsub("全新","",input_test)
  input_test<-gsub("新大7","大7",input_test)
  input_test<-gsub("级级","级",input_test)
  input_test<-gsub("奔驰M级","奔驰ML级",input_test)
  input_test<-gsub("中华尊驰","尊驰",input_test)
  input_test<-gsub("中华骏捷","骏捷",input_test)
  input_test<-gsub("中华酷宝","酷宝",input_test)
  input_test<-gsub("起亚K4","起亚凯绅",input_test)
  input_test<-gsub("陆地巡洋舰","兰德酷路泽",input_test)
  input_test<-gsub("ASX劲炫|劲炫ASX|劲炫","劲炫ASX",input_test)
  input_test<-gsub("赛威SLS","SLS赛威",input_test)
  input_test<-gsub("宝马M系","宝马",input_test)
  input_test<-gsub("长安商用V5","长安V5",input_test)
  input_test<-gsub("MGGS","MG锐腾",input_test)
  input_test<-gsub("MGGT","MG锐行",input_test)
  input_test<-gsub("MGHS","MG荷尔蒙",input_test)
  input_test<-gsub("MURANO","楼兰",input_test)
  input_test<-gsub("斯巴鲁WRX","斯巴鲁翼豹",input_test)
  input_test<-gsub("F-150","F150",input_test)
  input_test<-gsub("F-350","F350",input_test)
  input_test<-gsub("F-450","F450",input_test)
  input_test<-gsub("F-550","F550",input_test)
  input_test<-gsub("F-650","F650",input_test)
  input_test<-gsub("马自达马3","马自达3",input_test)
  input_test<-gsub("马自达马2","马自达2",input_test)
  input_test<-gsub("MAZDA马自达|MAZDA|马自达","马自达",input_test)
  input_test<-gsub("众泰郎朗|众泰朗骏","众泰",input_test)
  input_test<-gsub("优6SUV|优6","优6SUV",input_test)
  input_test<-gsub("小康小康","小康",input_test)
  input_test<-gsub("威旺威旺","威旺",input_test)
  input_test<-gsub("吉利全球鹰吉利|吉利全球鹰","吉利全球鹰吉利",input_test)
  input_test<-gsub("长安商用长安|长安商用","长安商用长安",input_test)
  input_test<-gsub("大7-SUV","大7SUV",input_test)
  input_test<-gsub("大7-MPV","大7MPV",input_test)
  input_test<-gsub("奔驰GT级AMG","AMGGT",input_test)
  input_test<-gsub("塞纳SIENNA|SIENNA","塞纳",input_test)
  input_test<-gsub("依维柯Venice|Venice","威尼斯",input_test)
  input_test<-gsub("福特房车E","福特E350",input_test)
  input_test<-gsub("奔驰GT级AMG","AMGGT",input_test)
  input_test<-gsub("吉利EC","吉利帝豪EC",input_test)
  input_test<-gsub("吉利C5","吉利英伦C5",input_test)
  input_test<-gsub("神行者2|神行者","神行者2",input_test)
  input_test<-gsub("CRV","CR-V",input_test)
  input_test<-gsub("吉利C5","吉利英伦C5",input_test)
  input_test<-gsub("吉利SC","吉利英伦SC",input_test)
  input_test<-gsub("SC5RV","SC5-RV",input_test)
  input_test<-gsub("金杯霸道SUV|金杯霸道","金杯霸道SUV",input_test)
  input_test<-gsub("吉利C5","吉利英伦C5",input_test)
  input_test<-gsub("制胜","致胜",input_test)
  input_test<-gsub("一汽夏利","夏利夏利",input_test)
  input_test<-gsub("北汽JEEP","北京JEEP",input_test)
  input_test<-gsub("众泰X7","众泰大迈X7",input_test)
  input_test<-gsub("桑塔纳3000","桑塔纳经典",input_test)
  input_test<-gsub("纳智捷5","纳5",input_test)
  input_test<-gsub("卡宴","CAYENNE",input_test)
  input_test<-gsub("卡曼","CAYMAN",input_test)
  input_test<-gsub("雅致|ARNAGE","雅致ARNAGE",input_test)
  input_test<-gsub("大众R系","大众",input_test)
  input_test<-gsub("东风A","风神A",input_test)
  input_test<-gsub("TACOMA","塔科马",input_test)
  input_test<-gsub("C-MAX","麦柯斯",input_test)
  input_test<-gsub("奔驰C[0-9]","奔驰C级",input_test)
  input_test<-gsub("奔驰E[0-9]","奔驰E级",input_test)
  input_test<-gsub("长安奔奔","长安轿车奔奔",input_test)
  input_test<-gsub("MULTIVAN","迈特威",input_test)
  input_test<-gsub("KUGA","翼虎",input_test)
  input_test<-gsub("AGERA","科尼赛克AGERA",input_test)
  input_test<-gsub("北汽新能源","北汽新能源北汽",input_test)
  input_test<-gsub("江铃新能源","江铃新能源江铃",input_test)
  input_test<-gsub("华泰新能源","华泰新能源华泰",input_test)
  input_test<-gsub("蔚来汽车","蔚来汽车蔚来",input_test)
  input_test<-gsub("君马汽车","君马汽车君马",input_test)
  input_test<-gsub("成功汽车","成功汽车成功",input_test)
  input_test<-gsub("起亚RIO","起亚锐欧",input_test)
  input_test<-gsub("经典圣达菲","圣达菲经典",input_test)
  input_test<-gsub("TOWNCAR","城市",input_test)
  input_test<-gsub("CELICA赛利卡|CELICA","赛利卡",input_test)
  input_test<-gsub("MAREA马力昂|MAREA","马力昂",input_test)
  input_test<-gsub("日产TITAN|TITAN","日产泰坦",input_test)
  input_test<-gsub("TERRACAN","特拉卡",input_test)
  input_test<-gsub("双环S-RV","双环来宝S-RV",input_test)
  input_test<-gsub("STREAM思韵|思韵STREAM|STREAM","思韵",input_test)
  input_test<-gsub("ODYSSEY奥德赛|奥德赛ODYSSEY|ODYSSEY","奥德赛",input_test)
  input_test<-gsub("PICKUP","皮卡",input_test)
  input_test<-gsub("猎豹CT-5","猎豹CT5",input_test)
  input_test<-gsub("拉达LADA|LADA","拉达",input_test)
  input_test<-gsub("尼瓦Niva|Niva","尼瓦",input_test)
  input_test<-gsub("塞纳SIENNA|SIENNA","塞纳",input_test)
  input_test<-gsub("斯宾特Sprinter|Sprinter","斯宾特",input_test)
  input_test<-gsub("BRABUS 巴博斯|BRABUS","巴博斯",input_test)
  input_test<-gsub("MISTRA名图|名图MISTRA|MISTRA","名图",input_test)
  input_test<-gsub("雅致ARNAGE|ARNAGE雅致|ARNAGE","雅致",input_test)
  input_test<-gsub("福仕达新鸿达","福仕达鸿达",input_test)
  input_test<-gsub("！","!",input_test)
  input_test<-gsub("AXELA昂科赛拉|昂科赛拉AXELA|AXELA","昂克赛拉",input_test)
  input_test<-gsub("马自达(3|)昂克赛拉|昂克赛拉","马自达3昂克赛拉",input_test)
  input_test<-gsub("神行者2|神行者","神行者2",input_test)
  input_test<-gsub("SWM斯威|斯威SWM","斯威",input_test)
  input_test[grep("赛欧.*赛欧3",input_test)]<-sub("赛欧","",input_test[grep("赛欧.*赛欧3",input_test)])
  input_test[grep("赛欧200(1|2|3|4)|200(1|2|3|4).*赛欧",input_test)]<-"别克赛欧"
  #福特部分
  input_test<-gsub("(福特|)(F-150|F150)","福特F系列F150",input_test)
  input_test<-gsub("(福特|)(F-350|F350)","福特F系列F350",input_test)
  input_test<-gsub("(福特|)(F-550|F550)","福特F系列F550",input_test)
  input_test<-gsub("(福特|)(E-150|E150)","福特E系列E150",input_test)
  input_test<-gsub("(福特|)(E-250|E250)","福特E系列E250",input_test)
  input_test<-gsub("(福特|)(E-350|E350)","福特E系列E350",input_test)
  input_test<-gsub("(福特|)(E-450|E450)","福特E系列E450",input_test)
  input_test<-gsub("(福特|)(E-550|E550)","福特E系列E550",input_test)
  return(input_test)
}