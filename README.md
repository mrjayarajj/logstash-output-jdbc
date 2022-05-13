Jdbc input plugin 


<<<<<<< HEAD
Integration version: v1.0.1
Released on: 2021-09-20
Changelog
=======
## Example : 
```
outupt { 
         jdbc {
				jdbc_connection_string => "${mysql_jdbc_connection_string}"
				jdbc_user => "${mysql_jdbc_user}"
				jdbc_password => "${mysql_jdbc_password}"
				jdbc_driver_class => "com.mysql.jdbc.Driver"
				statement_filepath => [
						"${logstash_project_path}",
						"/app/app_module/sql/any_dml.sql"
				]
				pool_max => 1
				id => "uniquie_name_helps_in_log"
			}
}
```
>>>>>>> e15384c66f8038712d31ff3d2e05355a355e8a9a


For other versions, see the Versioned plugin docs.

Getting Help


For questions about the plugin, send mail to GlobalUserServiceDev@Staples.com. For bugs or feature requests, open an issue in JIRA. 

Description


This plugin was created to emit the data into any database with a JDBC interface from Spring & Logstash Output. You can periodically schedule ingestion using a cron syntax (see schedule setting) or run the query one time to load data into Logstash. Each row in the resultset becomes a single event. Columns in the resultset are converted into fields in the event.

https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-conn-props-ueConfigs.html
https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
https://stackoverflow.com/questions/44489407/mysql-jdbc-not-batching-queries-even-after-rewritebatchedstatements-true