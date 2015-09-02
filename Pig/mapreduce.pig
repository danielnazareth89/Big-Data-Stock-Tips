var_1 = LOAD 'hdfs:///pigdata/' USING PigStorage(',','-tagFile') ;
var_2 = FOREACH var_1 GENERATE (chararray)$0 AS filename, (chararray)$1 AS date, (double)$7 AS adjclose ;
var_3 = FOREACH var_2 GENERATE $0,$1,$2,SUBSTRING($1,0,4) AS (year:int),SUBSTRING($1,5,7) AS (month:int), SUBSTRING($1,8,10) AS (day:int);
var_4 = GROUP var_3 BY (filename,year,month);
var_5 = FOREACH var_4 {sorted = ORDER var_3 BY day DESC; last_day = LIMIT sorted 1; sorted2 = ORDER var_3 BY day ASC; first_day = LIMIT sorted2 1; GENERATE group,last_day,first_day;};

var_6 = FOREACH var_5 GENERATE group, last_day.adjclose AS last_day, first_day.adjclose as first_day;

var_7 = FOREACH var_6 GENERATE group, flatten(last_day) as last_day, flatten(first_day) as first_day;

var_8 = FOREACH var_7 GENERATE group.$0 AS filename, group.$1 AS year, group.$2 AS month, ($1-$2)/$2 as ROR;

var_9 = GROUP var_8 BY filename;

var_10 = FOREACH var_9 GENERATE  group,flatten(var_8.ROR) AS ROR,COUNT(var_8.ROR) as count, AVG(var_8.ROR) as average; 

var_11 = FOREACH var_10 GENERATE group as filename,count, (double) ((ROR-average))*((ROR-average)) as mean_square;

var_12 = GROUP var_11 BY filename;

var_13 = FOREACH var_12 GENERATE group as filename,var_11.count as count, (double) SUM(var_11.mean_square) as sum_square;

var_14 = FOREACH var_13 GENERATE filename, flatten(count.count) as count, sum_square;

var_15 = DISTINCT var_14;

var_16 = FOREACH var_15 GENERATE filename,(double) SQRT((sum_square)/(count-1)) as volatility;

filt = FILTER var_16 BY volatility is not null and volatility>0;

var_17 = ORDER filt BY volatility DESC;

var_18 = ORDER filt BY volatility ASC;

var_19 = LIMIT var_17 10;

var_20 = LIMIT var_18 10;

STORE var_19 into 'hdfs:///pigdata/pig_stocks_out/final_greatest' using PigStorage(',');
STORE var_20 into 'hdfs:///pigdata/pig_stocks_out/final_smallest' using PigStorage(',');
