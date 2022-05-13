# Logstash Java Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

The documentation for Logstash Java plugins is available [here](https://www.elastic.co/guide/en/logstash/6.7/contributing-java-plugin.html).


This plugin was created to emit the data into any database with a JDBC interface from Spring & Logstash Output. You can periodically schedule ingestion using a cron syntax (see schedule setting) or run the query one time to load data into Logstash. Each row in the resultset becomes a single event. Columns in the resultset are converted into fields in the event.

https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-conn-props-ueConfigs.html
https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
https://stackoverflow.com/questions/44489407/mysql-jdbc-not-batching-queries-even-after-rewritebatchedstatements-true