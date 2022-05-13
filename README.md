# Logstash Java Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

The documentation for Logstash Java plugins is available [here](https://www.elastic.co/guide/en/logstash/6.7/contributing-java-plugin.html).

## Step 1 : 
###### gradle.properties
```
LOGSTASH_CORE_PATH=/Users/Downloads/ELK/logstash-7.16.3
```

## Step 2 : 
###### build.sh
```
./gradlew gem
which will generate gem file
```

## Step 3 : 
###### installation to logstash
```
${logstash_path}/bin/logstash-plugin install --no-verify --local ${logstash_custom_plugin_path}/logstash-output-couchbase-1.0.1.gem
```


## Example : 
```
outupt { 
         jdbc {
				jdbc_connection_string => "${mysql_jdbc_connection_string}"
				jdbc_validate_connection => true
				jdbc_validation_timeout => 120
				jdbc_user => "${mysql_jdbc_user}"
				jdbc_password => "${mysql_jdbc_password}"
				jdbc_driver_class => "com.mysql.jdbc.Driver"
				statement_filepath => [
						"${logstash_project_path}",
						"/app/app_module/sql/any_dml.sql"
				]
				pool_max => 1
				id => "sdc_n_add"
			}
}
```


This plugin was created to emit the data into any database with a JDBC interface from Spring & Logstash Output. 
You can periodically schedule ingestion using a cron syntax (see schedule setting) or 
run the query one time to load data into Logstash. 
Each row in the resultset becomes a single event. 
Columns in the resultset are converted into fields in the event.

https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-conn-props-ueConfigs.html
https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
https://stackoverflow.com/questions/44489407/mysql-jdbc-not-batching-queries-even-after-rewritebatchedstatements-true