####################################################
#输出评分图
####################################################
--install.packages("ggplot2") #安装ggplot2
library(ggplot2)
evaluator<-read.table(file="/Users/xiaolongyuan/Documents/tmp/e.csv",header=TRUE,sep=",")
g<-ggplot(data=evaluator,aes(precision,recall,col=algorithm))
g<-g+geom_point(aes(size=3))
#g<-g+geom_text(aes(label=algorithm))
g<-g+scale_x_continuous(limits=c(0,1))+scale_y_continuous(limits=c(0,1))
g


job<-read.table(file="/Users/xiaolongyuan/Documents/workspace_idea/Recommend/src/main/resources/data/job.csv",header=FALSE,sep=",")
names(job)<-c("jobid","create_date","salary")

pv<-read.table(file="/Users/xiaolongyuan/Documents/workspace_idea/Recommend/src/main/resources/data/pv.csv",header=FALSE,sep=",")
names(pv)<-c("userid","jobid")

pv[which(pv$userid==974),]
j1<-job[job$jobid %in% c(106,173,82,188,78),];j1
job[job$jobid %in% c(145,121,98,19),]
job[job$jobid %in% c(145,89,19),]

avg08<-mean(j1$salary)*0.8;avg08



###
algorithm,difference,precision,recall
userLoglikelihood,0.23430802968313102,0.6424242424242422,0.4098360655737705
userCityBlock,0.5365593635741582,0.919580419580419,0.4371584699453552
userTanimoto,0.5514081961813223,0.6625766871165644,0.41803278688524603
itemLoglikelihood,0.5292923578600974,0.26229508196721296,0.26229508196721296
itemCityBlock,0.9234169847086843,0.02185792349726776,0.02185792349726776
itemTanimoto,0.9169838188910009,0.26229508196721296,0.26229508196721296