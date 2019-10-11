package io.global.comm.ot.session;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.EventloopTaskScheduler.Schedule;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.session.SessionStore;
import io.datakernel.time.CurrentTimeProvider;
import io.global.kv.api.KvClient;
import io.global.kv.api.KvItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public final class KvSessionStore<V> implements SessionStore<V>, EventloopService {
	public static final Duration DEFAULT_SESSION_LIFETIME = Duration.ofDays(30);
	public static final Schedule DEFAULT_CLEANUP_SCHEDULE = Schedule.ofPeriod(Duration.ofDays(1));

	private final KvClient<String, V> kvClient;
	private final String table;
	private final EventloopTaskScheduler cleanUpScheduler;

	private Duration sessionLifetime = DEFAULT_SESSION_LIFETIME;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private KvSessionStore(Eventloop eventloop, KvClient<String, V> kvClient, String table) {
		this.kvClient = kvClient;
		this.table = table;
		this.cleanUpScheduler = EventloopTaskScheduler.create(eventloop, this::cleanUp)
				.withSchedule(DEFAULT_CLEANUP_SCHEDULE);
	}

	public static <V> KvSessionStore<V> create(Eventloop eventloop, KvClient<String, V> kvClient, String table) {
		return new KvSessionStore<>(eventloop, kvClient, table);
	}

	public KvSessionStore<V> withSessionLifetime(Duration sessionLifetime) {
		this.sessionLifetime = sessionLifetime;
		return this;
	}

	public KvSessionStore<V> withCleanUpSchedule(Schedule cleanUpSchedule) {
		this.cleanUpScheduler.setSchedule(cleanUpSchedule);
		return this;
	}

	@Override
	public Eventloop getEventloop() {
		return cleanUpScheduler.getEventloop();
	}

	@Override
	@NotNull
	public Promise<Void> start() {
		return cleanUpScheduler.start();
	}

	@Override
	@NotNull
	public Promise<Void> stop() {
		return cleanUpScheduler.stop();
	}

	@Override
	public Promise<Void> save(String sessionId, V value) {
		long timestamp = now.currentTimeMillis();
		return kvClient.put(table, new KvItem<>(timestamp, sessionId, value));
	}

	@Override
	public Promise<@Nullable V> get(String sessionId) {
		long timestamp = now.currentTimeMillis();
		return kvClient.get(table, sessionId)
				.then(kvItem -> {
					if (kvItem == null) {
						return Promise.of(null);
					}
					if (kvItem.getTimestamp() + sessionLifetime.toMillis() < timestamp) {
						return kvClient.remove(table, sessionId)
								.map($ -> null);
					}
					V value = kvItem.getValue();
					return kvClient.put(table, new KvItem<>(timestamp, sessionId, value))
							.map($ -> value);
				});
	}

	@Provides
	public Promise<Void> remove(String sessionId) {
		return kvClient.remove(table, sessionId);
	}

	public Duration getSessionLifetime() {
		return sessionLifetime;
	}

	private Promise<Void> cleanUp() {
		long timestamp = now.currentTimeMillis();
		return kvClient.download(table)
				.then(supplier -> supplier
						.filter(kvItem -> kvItem.getValue() != null && kvItem.getTimestamp() + sessionLifetime.toMillis() < timestamp)
						.map(KvItem::getKey)
						.streamTo(ChannelConsumer.ofPromise(kvClient.remove(table))));
	}

}
