library(data.table)
library(plyr)
library(class)
library(e1071)
library(gbm)

setwd("C:/Users/1/James/Research/Projects/Zia Project/Code")

root_path <- 'Data/ISWC_freq+dup20+window9s+40s/'
cpath <- 'Data/samples/concurrency/'
fpath <- 'Data/samples/frequency/'

filenames <- list.files(root_path)
filepaths <- paste0(root_path, filenames)

con_names <- list.files(cpath)
freq_names <- list.files(fpath)
concurrency_path <- paste0(cpath,con_names)
frequency_path <- paste0(fpath,freq_names)

get_stream_data <- function(filepaths, filenames, num_col = 6,type = ''){
  
  files <- lapply(filepaths,function(i) fread(i,header = T, sep = ','))
  filename_features <- strsplit(as.character(filenames), split="[ ,+]+")
  file.frame <- data.frame(matrix(unlist(filename_features), ncol=num_col, byrow = TRUE),stringsAsFactors = F)
  
  #file.frame$time <-  file.info(_path)$ctime
  
  if (type == 'con'){
    names(file.frame) <- c('query','r','frequency','duplicate','engine')
    
  }
  
  else if (type == 'freq'){
    names(file.frame) <- c('query','r','frequency','duplicate','engine')
  }
  
  else{
    names(file.frame) <- c('name','r','frequency','duplicate','engine','query')
  }
  
  clean_r$engine <- as.factor(clean_r$engine)
  
  clean_r <- as.data.frame(apply(file.frame,2, function(i) {
    sapply(strsplit(i, split= "="), function(x) x[length(x)])
  }))
  
  if (type == 'freq' | type == 'con'){
    a <- unlist(strsplit(as.character(clean_r$engine), split= "[.]+"))
    clean_r$engine <- a[which(1:length(a) %% 2 == 1)]
  }
  
  dfList <- lapply(1:length(files),function(i) merge(clean_r[i,],files[i]))
  #df <- do.call("rbind", dfList,c('V1','latency-Q1','latency-Q2','latency-Q3','memory'))
  
  df<-rbindlist(dfList, use.names=TRUE, fill=TRUE)
  dfList[is.na(dfList)] <- 0
  
  #f <- data.frame()
  #df <- for (i in 1:length(dfList)){
  #      f <- rbind(f,dfList[[i]])
  #  }
  
  if (type != 'freq' ){
    
    df[is.na(df)] <- 0
    freq <- sapply(unlist((numeric_version(df$frequency))),as.numeric)
    df$freq <- freq[ freq > 100 ] 
    
  }
  
  return(df)
}

combine_all <- function(df, con_df, freq_df){
  dfList <- list(df, con_df, freq_df)
  dfn <- rbindlist(dfList, use.names=TRUE,fill=T)
  #dfn[is.na(dfn)] <- 0
  return(dfn)
}

df <- get_stream_data(filepaths= filepaths, filenames = filenames)
con_df <- get_stream_data(concurrency_path, con_names, num_col = 5, type = 'con')
freq_df <- get_stream_data(frequency_path, freq_names, num_col = 5, type = 'freq')

all_df <- combine_all(df,con_df, freq_df)
all_df$query <-  as.factor(unlist(strsplit(as.character(all_df$query), split= ".csv")))

library(ggplot2)
ggplot(data=all_df, aes(all_df$memory)) +
  geom_histogram(aes(y =..density..),
                 col="red",
                 fill='blue',
                 alpha = .2) +
  geom_density(col=2) +
  labs(title="Histogram for Queries for all engines") +
  labs(x="Queries", y="Count")


View(all_df)

##### ONLY ON DF ##########

latency.1 <- al_df[all_df$latency.Q1>0]
latency.2 <- df[df$latency.Q2>0]
latency.3 <- df[df$latency.Q3>0]

latency.1.model <- lm(latency.Q1 ~ frequency+V1+memory, latency.1)
latency.2.model <- lm(latency.Q2 ~ frequency+V1+memory, latency.2)
latency.3.model <- lm(latency.Q3 ~ frequency+V1+memory, latency.3)

memory.1.model <- lm(memory ~ frequency+latency.Q1, latency.1)
memory.2.model <- lm(memory~ frequency+latency.Q2, latency.2)
memory.3.model <- lm(memory ~ frequency+latency.Q3, latency.3)


all_df[is.na(all_df)] <- 0
csparql.engine <- all_df[all_df$engine=='csparql']
cqels.engine <- all_df[all_df$engine=='cqels']
engines.data <- all_df[,c(3,5:6,8:12)] 


engine.classification <- knn(train=engines.data,test=engines.data, cl = engines.data$engine)
svm_model <- svm(engine ~ ., data=engines.data)
sum(svm_model$fitted == engines.data$engine)/length(svm_model$fitted)
summary(svm_model)

CMAT <- matrix(nrow=2, ncol=2)
CORR <- c()

for (i in 1:length(engines.data$frequency)){
  
  data_Tst <- engines.data[i,]
  data_Trn <- engines.data[-i,]
  svm.model1 <- svm(engine ~ ., data = data_Trn, kernel = "radial",degree=2, cost = 2, gamma = 0.02)
  svm.pred1 <- predict(svm.model1, data_Tst[,-2])
  conMAT <- table(pred = svm.pred1, true = data_Tst$engine)
  CMAT <- sum(CMAT,conMAT)
  CORR[i] <- sum(diag(conMAT))
  
}
sum(CORR)/length(CORR)

################### ON ALL DATA ##################


train.model <- function(df, num_iters = 20){
  
  #engine.classification <- knn(train=engines.data,test=engines.data, cl = engines.data$engine)
  CMAT <- matrix(nrow=2, ncol=2)
  CORR <- c()
  
  for (i in 1:num_iters){
    
    test_on <- sample(1:length(df$frequency),100,TRUE)
    data_Tst <- engines.data[test_on,]
    data_Trn <- engines.data[-test_on,]
    svm.model1 <- svm(engine ~ ., data = data_Trn, kernel = "radial",degree=2, cost = 2, gamma = 0.02,type='C-classification',prob=T)
    svm.pred1 <- predict(svm.model1, data_Tst[,-3])
    conMAT <- table(pred = svm.pred1, true = data_Tst$engine)
    CMAT <- sum(CMAT,conMAT)
    CORR[i] <- sum(diag(conMAT))
    
  }
  
  print(sum(CORR)/length(CORR))
  return (svm.model1)
  
}

#svm_model <- svm(engine ~ ., data=engines.data)
#sum(svm_model$fitted == engines.data$engine)/length(svm_model$fitted)
#summary(svm_model)

csparql.engine <- all_df[which(df$engine=='csparql'),]
cqels.engine <- all_df[which(df$engine=='cqels'),]
engines.data <- all_df[,c(3:21)] 

svm.model <- train.model(all_df, 100)


######## Boosting algorithm classifier #########

gb.model <- gbm.fit(as.data.frame(all_df[,c(3:4,6:21)]), as.numeric(unlist(all_df[,5]))-1,offset = NULL,misc = NULL,
                    distribution = "bernoulli",
                    w = NULL,var.monotone = NULL,n.trees = 100,
                    interaction.depth = 1,n.minobsinnode = 10,
                    shrinkage = 0.001,bag.fraction = 0.5,nTrain = NULL,
                    train.fraction = NULL,keep.data = TRUE,verbose = TRUE,
                    var.names = NULL,response.name = "y",group = NULL)

