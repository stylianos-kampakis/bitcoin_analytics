data<-read.csv("C:\\bcdata.csv",sep=";")
data<-data[data$exchange!="litetree.com",]
data<-data[data$exchange!="btcchina.com",]
data<-data[data$exchange!="okcoin.com",]
data$index<-data$hour*60+data$minutes

yuanusd=0.16

data[data$exchange=="btcchina.com",]$bid=data[data$exchange=="btcchina.com",]$bid*yuanusd
data[data$exchange=="okcoin.com",]$bid=data[data$exchange=="okcoin.com",]$bid*yuanusd

data[data$exchange=="btcchina.com",]$ask=data[data$exchange=="btcchina.com",]$ask*yuanusd
data[data$exchange=="okcoin.com",]$ask=data[data$exchange=="okcoin.com",]$ask*yuanusd



#data<-data[data$index>1006,]
data<-data[data$index %% 1 ==0,]

library(ggplot2)

ggplot(data,aes(x=index,y=bid,colour=exchange))+geom_line()+
  geom_line(aes(x=index,y=ask,linetype=exchange))+scale_x_continuous(breaks = round(seq(min(data$index), max(data$index), by = 30),1))+ theme(axis.text.x = element_text(angle = 90, hjust = 1))
#data2<-data[data$exchange=="bitstamp.com" | data$exchange=="bitfinex.com",]

#ggplot(data2,aes(x=index,y=bid,colour=exchange))+geom_line()+geom_line(aes(x=index,y=ask,linetype=exchange))

databitstamp<-data[data$exchange=="bitstamp.com",]
databitstamp<-databitstamp[,2:3]
names(databitstamp)<-c("bid_bistamp","ask_bitstamp")

databitfinex<-data[data$exchange=="bitfinex.com",]
databitfinex<-databitfinex[,2:3]
names(databitfinex)<-c("bid_bitfinex","ask_bitfinex")

datakraken<-data[data$exchange=="kraken.com",]
datakraken<-datakraken[,2:3]
names(datakraken)<-c("bid_kraken","ask_kraken")

datahitbtc<-data[data$exchange=="hitbtc.com",]
datahitbtc<-datahitbtc[,2:3]
names(datahitbtc)<-c("bid_hitbtc","ask_hitbtc")

databtcchina<-data[data$exchange=="btcchina.com",]
databtcchina<-databtcchina[,2:3]
names(databtcchina)<-c("bid_btcchina","ask_btcchina")

dataokcoin<-data[data$exchange=="okcoin.com",]
dataokcoin<-dataokcoin[,2:3]
names(dataokcoin)<-c("bid_okcoin","ask_okcoin")

rd<-cbind(databitstamp,databitfinex,datahitbtc,databtcchina,dataokcoin)
t<-ts(rd)
t<-diff(t,differences=2)
plot.ts(t)

VARselect(t, lag.max=12, type="both")$selection
VARselect(t, lag.max=12, type="both")
#VAR(t)
cor(rd)
