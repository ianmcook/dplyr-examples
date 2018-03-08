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

drv <- odbc::odbc()
impala <- src_impala(drv = drv, dsn = "Impala DSN")
flights_impala <- tbl(impala, "flights")


## top-N queries

# what are the 5 airports with the worst departure
# delays for flights departing JFK?

# the wrong way:
flights_impala %>% 
  group_by(origin, dest) %>% 
  summarize(
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  ) %>% 
  arrange(desc(avg_dep_delay)) %>% 
  filter(origin == "JFK") %>% 
  head(5)

# the right way:
flights_impala %>% 
  filter(origin == "JFK") %>% 
  group_by(dest) %>% 
  summarize(
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
   ) %>% 
  arrange(desc(avg_dep_delay)) %>% 
  head(5)

# another right way:
flights_impala %>% 
  filter(origin == "JFK") %>% 
  group_by(dest) %>% 
  summarize(
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  ) %>% 
  filter(
    min_rank(desc(avg_dep_delay)) <= 5 # window function
  ) %>%
  arrange(desc(avg_dep_delay))

# see details about window functions in dplyr at
# https://cran.r-project.org/package=dplyr/vignettes/window-functions.html

dbDisconnect(impala)
