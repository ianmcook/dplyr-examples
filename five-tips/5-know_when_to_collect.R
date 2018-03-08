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


# To visualize data from a remote source, you must 
# first return the remote data to R. To do this, use
# the collect() function.

# When working with large data, you typically need to
# sample, filter, or aggregate before using collect().

# Example: Select the departure delay and arrival delay
# columns, randomly sample 5% of non-missing records, 
# and return the result to R.

# using sparklyr:
delays_sample <- flights_spark %>%
  select(dep_delay, arr_delay) %>%
  na.omit() %>%
  sdf_sample(fraction = 0.05, replacement = FALSE) %>%
  collect()

# using implyr:
delays_sample <- flights_impala %>%
  select(dep_delay, arr_delay) %>%
  na.omit() %>%
  filter(rand(unix_timestamp()) < 0.05) %>%
  collect()


# visualize the result:
library(ggplot2)
ggplot(delays_sample, aes(x=dep_delay, y=arr_delay)) +
  geom_point()


# another way:
# use dbplot
# http://db.rstudio.com/dbplot/


dbDisconnect(impala)
spark_disconnect(spark)
