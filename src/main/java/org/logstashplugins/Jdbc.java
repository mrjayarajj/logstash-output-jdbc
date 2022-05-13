package org.logstashplugins;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jruby.RubyFixnum;
import org.jruby.RubyNil;
import org.jruby.RubyString;
import org.logstash.ConvertedList;
import org.logstash.ConvertedMap;
import org.logstash.ext.JrubyTimestampExtLibrary.RubyTimestamp;

import com.google.common.base.Joiner;
import com.google.common.collect.Queues;
import com.google.common.io.Files;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.Password;
import co.elastic.logstash.api.PluginConfigSpec;
import lombok.extern.slf4j.Slf4j;

// class name must match plugin name
@LogstashPlugin(name = "jdbc")
@Slf4j
public class Jdbc implements Output {

	public static final PluginConfigSpec<String> JDBC_CONNECTION_STRING_CONFIG = PluginConfigSpec
			.requiredStringSetting("jdbc_connection_string");
	public static final PluginConfigSpec<String> JDBC_USER_CONFIG = PluginConfigSpec.requiredStringSetting("jdbc_user");
	public static final PluginConfigSpec<Password> JDBC_PASSWORD_CONFIG = PluginConfigSpec
			.passwordSetting("jdbc_password", null, false, true);
	public static final PluginConfigSpec<List<Object>> STATEMENT_FILEPATH_CONFIG = PluginConfigSpec
			.arraySetting("statement_filepath");
	public static final PluginConfigSpec<String> JDBC_DRIVER_CLASS_CONFIG = PluginConfigSpec
			.requiredStringSetting("jdbc_driver_class");
	public static final PluginConfigSpec<Long> JDBC_COMMIT_SIZE_CONFIG = PluginConfigSpec.numSetting("jdbc_commit_size",
			1000);
	public static final PluginConfigSpec<Long> POOL_MAX_CONFIG = PluginConfigSpec.numSetting("pool_max",
			Runtime.getRuntime().availableProcessors());
	public static final PluginConfigSpec<String> ID_CONFIG = PluginConfigSpec.stringSetting("id");

	private final String id;

	private List<Object> jdbc_filepath;
	private String jdbc_connection_string;
	private String jdbc_user;
	private Password jdbc_password;
	private String jdbc_driver_class;
	private int jdbc_commit_size;
	private int poolMax;

	private String sql = null;

	private final CountDownLatch done = new CountDownLatch(1);

	public AtomicInteger serviceDone = new AtomicInteger();

	private volatile boolean stopped = false;

	private ArrayBlockingQueue<Event> arrayBlockingQueue;

	private int secondWait = 1;

	private JdbcService jdbcService;

	// all plugins must provide a constructor that accepts id, Configuration, and
	// Context
	public Jdbc(final String id, final Configuration config, final Context context) {

		// constructors should validate configuration options
		this.id = id;

		jdbc_connection_string = config.get(JDBC_CONNECTION_STRING_CONFIG);
		jdbc_driver_class = config.get(JDBC_DRIVER_CLASS_CONFIG);
		jdbc_user = config.get(JDBC_USER_CONFIG);
		jdbc_password = config.get(JDBC_PASSWORD_CONFIG);
		jdbc_filepath = config.get(STATEMENT_FILEPATH_CONFIG);
		jdbc_commit_size = config.get(JDBC_COMMIT_SIZE_CONFIG).intValue();
		poolMax = config.get(POOL_MAX_CONFIG).intValue();

		log.info("");
		log.info("logstash-output-jdbc version : 1.0.1, arr ");
		log.info("Jdbc Output Plugin Id :" + id);
		log.info("Jdbc Connection String :" + jdbc_connection_string);
		log.info("Jdbc Driver Class :" + jdbc_driver_class);
		log.info("Jdbc User :" + jdbc_user);
		log.info("Jdbc Filepath :" + jdbc_filepath);
		log.info("Jdbc Commit Size :" + jdbc_commit_size);
		log.info("Jdbc Pool Max :" + poolMax);
		log.info("");

		try {
			String filePath = Joiner.on("").join(jdbc_filepath);
			sql = Files.asCharSource(new File(filePath), StandardCharsets.UTF_8).read();
			log.info("sql : {} from : {} ", sql, filePath);
		} catch (Exception e) {
			log.error("sql not found");
			throw new RuntimeException(e);
		}

		arrayBlockingQueue = new ArrayBlockingQueue<Event>(jdbc_commit_size*poolMax);

		jdbcService = new JdbcService(jdbc_connection_string, jdbc_driver_class, jdbc_user,
				jdbc_password.getPassword());

		log.info("starting {} consumer threads ", poolMax);

		while (poolMax-- != 0) {

			log.info("creating consumer thread {} ", poolMax);

			// Consumer thread
			new Thread(() -> {
				try {
					while (true) {

						log.debug("scaning for items to drain...");

						List<Event> l = new ArrayList<Event>();

						Queues.drain(arrayBlockingQueue, l, jdbc_commit_size, secondWait, TimeUnit.SECONDS);

						if (l.size() > 0) {
							log.info("queue filled up by {} or up to the specified timeout {}s is {} ",
									jdbc_commit_size, secondWait, l.size());

							serviceDone.incrementAndGet();

							List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();

							for (Event event : l) {
								ConvertedMap convertedMap = (ConvertedMap) event.getData();
								log.debug("convertedMap : " + convertedMap);
								Map<String, Object> convertedJavaMap = asJava(convertedMap);
								log.debug("convertedJavaMap : " + convertedJavaMap);
								items.add(convertedJavaMap);
							}

							try {
								jdbcService.save(sql, items);
							} catch (Exception e) {
								/*
								 * l.stream().forEach(event -> { try { log.info("writing to dlq  ");
								 * DeadLetterQueueFactory.getWriter(id,
								 * "/var/opt/appworkr/logstash_data/data/dead_letter_queue", 100000)
								 * .writeEntry((org.logstash.Event) event, "US02_gus_defaultuser",
								 * "US02_gus_defaultuser", "resn");
								 * 
								 * } catch (Exception e1) { log.error("error while writing to DLQ {} ", event);
								 * } });
								 */
							}

							serviceDone.decrementAndGet();

						}

					}

				} catch (InterruptedException e) {
					log.warn("consumer thread was interrupted", e);
				}

			}, id + "-drain-" + poolMax).start();
		}
	}

	private Map<String, Object> asJava(ConvertedMap covertedMap) {

		Map<String, Object> convertedJavaMap = new HashMap<>();

		covertedMap.keySet().forEach(key -> {
			if (covertedMap.get(key) instanceof RubyNil) {
				convertedJavaMap.put(key, null);
			} else if (covertedMap.get(key) instanceof RubyFixnum) {
				RubyFixnum str = (RubyFixnum) covertedMap.get(key);
				convertedJavaMap.put(key, str.getLongValue());
			} else if (covertedMap.get(key) instanceof RubyString) {
				RubyString str = (RubyString) covertedMap.get(key);
				convertedJavaMap.put(key, str.asJavaString());
			} else if (covertedMap.get(key) instanceof RubyTimestamp) {
				RubyTimestamp ts = (RubyTimestamp) covertedMap.get(key);
				convertedJavaMap.put(key, ts.getTimestamp().getTime().toDate());
			}  else if (covertedMap.get(key) instanceof ConvertedList) {
				ConvertedList cl = (ConvertedList) covertedMap.get(key);
				convertedJavaMap.put(key, cl.unconvert());
			} else if (covertedMap.get(key) instanceof ConvertedMap) {
				convertedJavaMap.put(key, asJava((ConvertedMap) covertedMap.get(key)));
			} else {
				convertedJavaMap.put(key, covertedMap.get(key));
			}
		});

		return convertedJavaMap;
	}

	@Override
	public void output(final Collection<Event> events) {

		Iterator<Event> z = events.iterator();

		while (!events.isEmpty() && z.hasNext() && !stopped) {

			Event event = z.next();

			try {
				if (!stopped) {
					arrayBlockingQueue.put(event);
				} else {
					while (true) {
						log.info(
								"stop signal received so don't fill up the queue but wait 1sec until someone shutdown..");
						try {
							TimeUnit.SECONDS.sleep(1);
						} catch (InterruptedException e1) {
							log.error("interrupted while producer wait after stop signal");
						}
					}
				}
			} catch (InterruptedException e1) {
				log.error("interrupted while filling the queue");
			}

		}

	}

	@Override
	public void stop() {

		log.info("stop() method is called ..");
		log.info("arrayBlockingQueue.size() : " + arrayBlockingQueue.size());

		log.info("updating the stopped status from {} to true ", stopped);
		stopped = true;

		while (serviceDone.get() != 0) {
			log.info("there are {} service in progress so waiting... ", serviceDone);
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				log.warn("stop wait was interrupted", e);
			}
		}

		log.info("all services are done safe to shutdown");

		done.countDown();
	}

	@Override
	public void awaitStop() throws InterruptedException {
		log.info("awaitStop() method is called ..");
		done.await();
	}

	@Override
	public Collection<PluginConfigSpec<?>> configSchema() {
		// should return a list of all configuration options for this plugin
		return Arrays.asList(JDBC_CONNECTION_STRING_CONFIG, JDBC_USER_CONFIG, JDBC_PASSWORD_CONFIG,
				STATEMENT_FILEPATH_CONFIG, JDBC_DRIVER_CLASS_CONFIG, JDBC_COMMIT_SIZE_CONFIG, POOL_MAX_CONFIG,
				ID_CONFIG);
	}

	@Override
	public String getId() {
		return id;
	}

}
