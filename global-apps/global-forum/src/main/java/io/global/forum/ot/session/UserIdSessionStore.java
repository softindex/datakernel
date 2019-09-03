package io.global.forum.ot.session;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.EventloopTaskScheduler.Schedule;
import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.time.CurrentTimeProvider;
import io.global.forum.ot.session.operation.AddOrRemoveSession;
import io.global.forum.ot.session.operation.SessionOperation;
import io.global.forum.ot.session.operation.UpdateTimestamp;
import io.global.forum.pojo.UserId;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;

import static io.global.forum.ot.session.operation.AddOrRemoveSession.add;
import static java.util.stream.Collectors.toList;

public final class UserIdSessionStore implements SessionStore<UserId>, EventloopService {
	public static final Duration DEFAULT_SESSION_LIFETIME = Duration.ofDays(30);
	public static final Schedule DEFAULT_CLEANUP_SCHEDULE = Schedule.ofPeriod(Duration.ofDays(1));

	private final OTStateManager<CommitId, SessionOperation> stateManager;
	private final Map<String, UserIdSession> sessions;
	private final EventloopTaskScheduler cleanUpScheduler;

	private Duration sessionLifetime = DEFAULT_SESSION_LIFETIME;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private UserIdSessionStore(OTStateManager<CommitId, SessionOperation> stateManager, Map<String, UserIdSession> sessions) {
		this.stateManager = stateManager;
		this.sessions = sessions;
		this.cleanUpScheduler = EventloopTaskScheduler.create(stateManager.getEventloop(), this::cleanUp)
				.withSchedule(DEFAULT_CLEANUP_SCHEDULE);
	}

	public static UserIdSessionStore create(OTStateManager<CommitId, SessionOperation> stateManager) {
		return new UserIdSessionStore(stateManager, ((SessionOTState) stateManager.getState()).getSessions());
	}

	public UserIdSessionStore withSessionLifetime(Duration sessionLifetime) {
		this.sessionLifetime = sessionLifetime;
		return this;
	}

	public UserIdSessionStore withCleanUpSchedule(Schedule cleanUpSchedule) {
		this.cleanUpScheduler.setSchedule(cleanUpSchedule);
		return this;
	}

	@Override
	public Eventloop getEventloop() {
		return stateManager.getEventloop();
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
	public Promise<Void> save(String sessionId, UserId userId) {
		long timestamp = now.currentTimeMillis();
		UserIdSession userIdSession = sessions.get(sessionId);
		if (userIdSession != null) {
			return Promise.complete();
		}
		stateManager.add(add(sessionId, userId, timestamp));
		return stateManager.sync();
	}

	@Override
	public Promise<UserId> get(String sessionId) {
		UserIdSession sessionObject = sessions.get(sessionId);
		if (sessionObject == null) {
			return Promise.of(null);
		}
		long timestamp = now.currentTimeMillis();
		if (sessionObject.getLastAccessTimestamp() + sessionLifetime.toMillis() < timestamp) {
			stateManager.add(AddOrRemoveSession.remove(sessionId, sessionObject.getUserId(), sessionObject.getLastAccessTimestamp()));
			return stateManager.sync()
					.map($ -> null);
		}
		stateManager.add(UpdateTimestamp.update(sessionId, sessionObject.getLastAccessTimestamp(), timestamp));
		return stateManager.sync()
				.map($ -> sessionObject.getUserId());
	}

	@Provides
	public Promise<Void> remove(String sessionId) {
		UserIdSession session = sessions.get(sessionId);
		if (session == null) {
			return Promise.complete();
		}
		stateManager.add(AddOrRemoveSession.remove(sessionId, session.getUserId(), session.getLastAccessTimestamp()));
		return stateManager.sync();
	}

	public Duration getSessionLifetime() {
		return sessionLifetime;
	}

	private Promise<Void> cleanUp() {
		long timestamp = now.currentTimeMillis();
		stateManager.addAll(sessions.entrySet()
				.stream()
				.filter(entry -> entry.getValue().getLastAccessTimestamp() + sessionLifetime.toMillis() < timestamp)
				.map(entry -> {
					UserIdSession value = entry.getValue();
					return AddOrRemoveSession.remove(entry.getKey(), value.getUserId(), value.getLastAccessTimestamp());
				})
				.collect(toList()));
		return stateManager.sync();
	}

}
