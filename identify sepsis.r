### sepsis-3 for eicu
rm(list=ls())
gc()
library(bootstrap)
library(boot)
library(readxl)
library(openxlsx)
library(stringi)
library(stringr)
library(data.table)
library(dplyr)
library(RPostgreSQL)
library(RODBC)
library(RPostgres)
'%&%' <- function(x,y){paste0(x,y)}

#### time series
time_series<-function(x){
    los_icuhs<-seq(0,unique(x$hours_icu),1)
    rowls<-length(los_icuhs)
    ids<-unique(x$patientunitstayid)
    sex<-unique(x$gender)
    ages<-unique(x$age)
    df<-data.frame("patientunitstayid"=rep(ids,rowls),"gender"=rep(sex,rowls),"age"=rep(ages,rowls))
    df<-cbind(df,los_icuhs)
    return(df)
}
trans_data<-function(data,id_vars){
    data<-as.data.table(data)
    dt1<-split(data,by=id_vars)
    dt2<-lapply(dt1,time_series)
    dt2<-rbindlist(dt2)
}
### 
bsdt1$hours_icu=round(bsdt1$unitdischargeoffset/60,0)
bsdt1%>%filter(hours_icu>0)->bsdt2
tfdt<-trans_data(data=bsdt2,id_vars="patientunitstayid")
#### merge these data
all_data=left_join(tfdt,labdt1,by=c("patientunitstayid","los_icuhs"="labresultoffset"))
all_data=left_join(all_data,vitals,by=c("patientunitstayid","los_icuhs"="observationoffset"))
save(all_data,file=kout %&% "long_合并数据.RData")

####4. 感染时刻 
re<-dbSendQuery(conn,"select * from eicuii.microlab")
microdt<-dbFetch(re)
dbClearResult(re)
microdt%>%select(patientunitstayid,culturetakenoffset)->sus_infection

####4.1 抗菌药物
abtio<-fread(kout %&% "medication抗菌药字段.csv",head=TRUE,stringsAsFactors=FALSE)
re<-dbSendQuery(conn,"select * from eicuii.medication")
medication<-dbFetch(re)
dbClearResult(re)

medication %>% select(patientunitstayid,drugstartoffset,drugname,routeadmin)->medication
medication<-left_join(medication,abtio,by=c("drugname","routeadmin"))
medication%>%filter(antibiotic==1)->antibiotics
save(antibiotics,file=kout %&%"抗菌药物使用.RData")

###4.3 合并统一感染日期
sus_infection<-full_join(sus_infection,antibiotics%>%distinct(patientunitstayid,drugstartoffset),by=c("patientunitstayid","culturetakenoffset"="drugstartoffset"),relationship ="many-to-many")
save(sus_infection,file=kout %&%"抗菌药及微生物检查日期.RData")

###判断SOFA
load(kdt %&% "eicu_sofa_hourly.RData")

rts%>%select(patientunitstayid,hr,sofa_24hours)%>%data.table()->sofas
sofal<-split(sofas,by="patientunitstayid")
### rule1, sofa>=2,最终结果 13154人
sofas%>%filter(sofa_24hours>=2)->sofa

sepsis1<-inner_join(sus_infection,sofa,by="patientunitstayid",relationship ="many-to-many")
sepsis1%>%filter(hr>=culturetakenoffset-48 & hr<=culturetakenoffset+24)->sepsis2

### rule2. sofa increase >=2
sofal<-split(sofas,by="patientunitstayid")

incrs<-function(x){
    sof_df<-data.frame(patientunitstayid="",hr="",sofa_24hours="")
    for(v in x$hr){
        tmpdt<-x%>%filter(hr<=v+24)
        vmax<- max(tmpdt$sofa_24hours)
        vmin<- min(tmpdt$sofa_24hours)
        if(vmax-vmin >=2){
            x%>%filter(sofa_24hours==vmax)%>%filter(hr==min(hr))->x1
            sof_df<-rbind(sof_df,x1)
        }
    }
    sof_df%>%unique()->sof_df
    return(sof_df)
}

sofal1<-lapply(sofal,incrs)###执行完毕

load(file=kout %&%"sofa增加2分数据.RData")##直接读取sofal1,获取突然增加2分的sofa评分患者
rbindlist(sofal1)->sofa2
sus_infection$patientunitstayid<-as.character(sus_infection$patientunitstayid)
sepsis1<-inner_join(sus_infection,sofa2,by="patientunitstayid",relationship ="many-to-many")
sepsis1%>%filter(hr>=culturetakenoffset-48 & hr<=culturetakenoffset+24)->sepsis2  ### 9329人
