###
library(magrittr)

## Initialize spark
library("SparkR", lib.loc="~/local/spark/R/lib")
ss <- sparkR.session(master="local[2]")
                           # invoke spark session on local computer,
                           # 2 executors (cpus)

## read data
wvs <-
   read.df("/home/siim/tyyq/andmebaasiq/WVS/wv6/WV6_Data_ascii_delimited_v_2016_01_01.dat.bz2", source="csv", header="true") %>%
                           # read data as SparkDataFrame
   select(c("V2", "V127", "V240", "V242", "V248", "V253")) %>%
   rename(country=column("V2"), leader=column("V127"),
          male=column("V240"), age=column("V242"),
          edu=column("V248"), townsize=column("V253")) %>%
                           # V2: country/region
                           # v127: having a strong leader is: 1 good, 4
                           # bad
                           # v240: gender, 1=M, 2=F
                           # v242: age
                           # v253: town size
   withColumn("male", column("male") == 1) %>%
   filter(column("leader") > 0) %>%
                           # only take valid respones to the leader
                           # question
   withColumn("age", column("age") + 0) %>%
   filter(column("age") > 14) %>%
   withColumn("age2", column("age")^2) %>%
#   withColumn("edu", column("edu") + 0) %>%
   filter(column("edu") > 0) %>%
   filter(column("townsize") > 0)

## Show a little bit of data
cat("WVS6:", nrow(wvs), "observations.  Sample:\n")
wvs %>% showDF(8)
wvs %>% describe %>% showDF
wvs %>% groupBy("edu") %>% count %>% showDF
wvs %>% groupBy("townsize") %>% count %>% showDF

## Run linear regression
model <- wvs %>%
   glm(formula=leader ~ male + age + age2 + edu
       + townsize
       + country
       , family=gaussian(),
       data=.)
                           # linear model: how much does 'country'
                           # explain of the leader attitude
model %>% summary %>% print


