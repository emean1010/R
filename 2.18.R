library(RODBC)
library(proto)
library(gsubfn)
library(RSQLite)
library(sqldf)
library(data.table)
library(readr)
library(dplyr)
library(tidyr)
library(lubridate)
library(plyr)
setwd("C:/R")
data0<-as.data.table(read_csv("./oid161107.csv")) #载入16年订单数据
data1<-as.data.table(read_csv("./oid1702.csv")) #载入17年订单数据
data2<-as.data.table(read_csv("./did161107.csv")) #载入16年子订单数据
data3<-as.data.table(read_csv("./did1702.csv")) #载入17年子订单数据
oid01<-rbind(data0,data1) #合并订单数据
did01<-rbind(data2,data3) #合并子订单数据

oid01[,sx:=1] #反结标记列，默认值1
oid01[cancelnewoid>0,sx:=0] #标记反结订单为0
oid02<-oid01[,cancelnewoid:=NULL] #删除cancelnewoid列 
oid02<-oid02[sx==1] #删除反结订单项
oid02[,newtime:=NULL]
oid03<-separate(oid02,newtime,c("day","time"),sep = " ",remove = TRUE) #订单日期时间分离
# 添加时段列“tl”，宵夜：xy，午市：wu，晚市：wan
oid03[time<"08:00:00",tl:="xy"] #运行时弹出警告，再次运行
oid03[time>="08:00:00"&time<"17:00:00",tl:="wu"]
oid03[time>="17:00:00"&time<"22:30:00",tl:="wan"]
oid03[time>="22:30:00",tl:="xy"]
oid03[,":="(day=NULL,time=NULL)] #删除时间列

did02<-separate(did01,ordertime,c("day","time"),sep = " ",remove = TRUE) #子订单日期时间分离
did02[,time:=NULL]
did03<-did02[,lapply(.SD,sum),by=.(slsid,did,day,oid)] #同一oid保留唯一did项，数量和实收汇总求和

data4<-merge(did03,oid03,by=c("slsid","oid")) #合并订单和子订单数据，去除子订单中反结项
zhou<-as.data.table(read_csv("./zhou.csv")) #载入周数表，添加周数“week”列
shop<-as.data.table(read_csv("./sls_shop.csv")) #载入店铺信息表，添加“city”，“area”列
data5<-merge(data4,shop,by = "slsid") #添加店铺信息
data6<-merge(data5,zhou,by="day") #添加周数
data7<-data6[,danshu:=count(n_distinct(oid)),by=.(slsid,week,tl)] #统计订单数
data8<-data7[,":="(dishnum=sum(sx),money=sum(oicost),num=sum(amount)),by=.(slsid,week,tl,did)] #统计菜单数
data9<-unique(data8[,":="(day=NULL,amount=NULL,oicost=NULL,sx=NULL,oid=NULL)]) #用于计算销售额，毛利率，单店点击率

data10<-data9[,":="(slsid=NULL,tl=NULL,shopname=NULL,city=NULL)]
data11<-data10[,lapply(.SD,sum),by=.(did,area,week)]
data12<-data11[,danshu:=max(danshu),by=.(area,week)]
dish_name<-as.data.table(read_csv("./dish_name.csv"))
data12<-merge(data12,dish_name,by="did")
write_csv(data12,"./area2.25.csv")

data13<-data9[,":="(slsid=NULL,tl=NULL,shopname=NULL,area=NULL)]
data14<-data13[,lapply(.SD,sum),by=.(did,city,week)]
data15<-data14[,danshu:=max(danshu),by=.(city,week)]
data15<-merge(data15,dish_name,by="did")
data15[,djl:=dishnum/danshu]
write_csv(data15,"./city2.24.csv")
