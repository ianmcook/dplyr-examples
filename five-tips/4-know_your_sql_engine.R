# Copyright 2018 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

library(implyr)
library(sparklyr)
library(dplyr)

drv <- odbc::odbc()
impala <- src_impala(drv = drv, dsn = "Impala DSN")
flights_impala <- tbl(impala, "flights")

spark <- spark_connect(master = "yarn")
flights_spark <- tbl(spark, "flights")


## counting distinct values

# the usual way:
flights_impala %>% 
  group_by(origin) %>% 
  summarise(appx_num_dest = n_distinct(dest)) %>%
  arrange(desc(appx_num_dest))

# more efficient:
flights_impala %>% 
  group_by(origin) %>% 
  summarise(appx_num_dest = NDV(dest)) %>%
  arrange(desc(appx_num_dest))


## calculating the median (50th percentile)

# implyr translates median() to APPX_MEDIAN()
flights_impala %>%
  filter(dest %in% c("SJC", "SFO")) %>%
  group_by(origin, dest) %>%
  summarise(
    num_departures = n(),
    median_dep_delay = median(dep_delay)
  ) %>%
  arrange(median_dep_delay) %>%
  show_query()

# with sparklyr, you need to use percentile(column, 0.5)
# or percentile_approx(column, 0.5)
flights_spark %>%
  filter(dest %in% c("SJC", "SFO")) %>%
  group_by(origin, dest) %>%
  summarise(
    num_departures = n(),
    median_dep_delay = percentile_approx(dep_delay, 0.5)
  ) %>%
  arrange(median_dep_delay) %>%
  show_query()


# see the HiveQL and Impala SQL function documentation:
# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
# https://www.cloudera.com/documentation/enterprise/latest/topics/impala_functions.html


dbDisconnect(impala)
spark_disconnect(spark)
