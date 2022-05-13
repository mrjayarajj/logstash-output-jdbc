package org.logstashplugins;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StopWatch;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcService {

	JdbcService(String jdbc_connection_string, String jdbc_driver_class, String jdbc_user, String jdbc_password) {

		BasicDataSource dbcp = new BasicDataSource();
		dbcp.setDriverClassName(jdbc_driver_class);
		dbcp.setUrl(jdbc_connection_string);
		dbcp.setUsername(jdbc_user);
		dbcp.setPassword(jdbc_password);

		namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dbcp);
	}

	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	private static final String REGEX = ":(?<param>[a-zA-Z0-9._]*)";

	public void save(String sql, List<Map<String, Object>> items) throws Exception {

		List<Map<String, Object>> batchValuesList = new ArrayList<Map<String, Object>>();

		for (Map<String, Object> item : items) {

			Map<String, Object> parameters = new HashMap<String, Object>();

			Pattern pattern = Pattern.compile(REGEX);
			Matcher matcher = pattern.matcher(sql);

			while (matcher.find()) {

				String k = matcher.group("param");
				Object v = item.get(k);

				log.debug(" k : {} , "//
						+ "v : {} , "//
						+ "v instanceof Long : {} , "//
						+ "v instanceof String : {}", k, //
						v, //
						v instanceof Long, //
						v instanceof String);

				if (v == null) {
					parameters.put(k, null);
				} else if (v instanceof Long) {
					parameters.put(k, (Long) v);
				} else if (v instanceof String) {
					parameters.put(k, (String) v);
				} else if (v instanceof Date) {
					parameters.put(k, (Date) v);
				} else if (v instanceof List) {
					parameters.put(k, (List) v);
				}
			}

			batchValuesList.add(parameters);

		}

		StopWatch timer = new StopWatch();
		try {
			timer.start();
			namedParameterJdbcTemplate.batchUpdate(sql, getArrayData(batchValuesList));
			timer.stop();
			log.info("saving to database completed for items {} in {} ms ", items.size(), timer.getTotalTimeMillis());
		} catch (Exception e) {
			log.error("error {} on sql_statement = {}  parameters = {} ", e.getMessage(), sql, batchValuesList.size());
			throw e;
		}

	}

	/*
	 * @SuppressWarnings("unchecked") private Observable<int[]>
	 * retry(Observable<int[]> o) { return o.retryWhen(//
	 * RetryBuilder.anyOf(DataAccessException.class, //
	 * ).delay(Delay.linear(TimeUnit.MILLISECONDS, 5000, 1, 500)) // wait before
	 * retry .max(100) // retry 100 times .build()); }
	 */

	public static Map<String, Object>[] getArrayData(List<Map<String, Object>> list) {
		@SuppressWarnings("unchecked")
		Map<String, Object>[] maps = new HashMap[list.size()];

		Iterator<Map<String, Object>> iterator = list.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			Map<String, Object> map = (Map<String, Object>) iterator.next();
			maps[i++] = map;
		}

		return maps;
	}
}
