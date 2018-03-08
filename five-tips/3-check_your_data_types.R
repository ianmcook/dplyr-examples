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


## filtering to literal values

# the wrong way:
flights_impala %>% filter(month == "3")

# the right way:
flights_impala %>% select(month) # notice month is an integer column
flights_impala %>% filter(month == 3L)

# another right way:
flights_impala %>% filter(as.integer(month) == 3L)


dbDisconnect(impala)
