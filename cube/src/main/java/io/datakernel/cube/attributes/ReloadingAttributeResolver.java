package io.datakernel.cube.attributes;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.ValueStats;

import java.util.HashMap;
import java.util.Map;

public abstract class ReloadingAttributeResolver<K, A> extends AbstractAttributeResolver<K, A> implements EventloopService {
	protected final Eventloop eventloop;

	private long timestamp;
	private long reloadPeriod;
	private long retryPeriod = 1000L;
	private ScheduledRunnable scheduledRunnable;
	private final Map<K, A> cache = new HashMap<>();
	private int reloads;
	private int reloadErrors;
	private int resolveErrors;
	private K lastResolveErrorKey;
	private final ValueStats reloadTime = ValueStats.create().withSmoothingWindow(60 * 60.0);

	protected ReloadingAttributeResolver(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Override
	protected final A resolveAttributes(K key) {
		A result = cache.get(key);
		if (result == null) {
			resolveErrors++;
			lastResolveErrorKey = key;
		}
		return result;
	}

	protected abstract void reload(@Nullable long lastTimestamp, ResultCallback<Map<K, A>> callback);

	private void doReload() {
		reloads++;
		scheduledRunnable.cancel();
		final long reloadTimestamp = getEventloop().currentTimeMillis();
		reload(timestamp, new ResultCallback<Map<K, A>>() {
			@Override
			protected void onResult(Map<K, A> result) {
				reloadTime.recordValue((int) (getEventloop().currentTimeMillis() - reloadTimestamp));
				cache.putAll(result);
				timestamp = reloadTimestamp;
				scheduleReload(reloadPeriod);
			}

			@Override
			protected void onException(Exception e) {
				reloadErrors++;
				scheduleReload(retryPeriod);
			}
		});
	}

	private void scheduleReload(long period) {
		Eventloop eventloop = getEventloop();
		scheduledRunnable = eventloop.schedule(eventloop.currentTimeMillis() + period, new Runnable() {
			@Override
			public void run() {
				doReload();
			}
		});
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		final long reloadTimestamp = getEventloop().currentTimeMillis();
		reload(timestamp, new ForwardingResultCallback<Map<K, A>>(callback) {
			@Override
			protected void onResult(Map<K, A> result) {
				reloadTime.recordValue((int) (getEventloop().currentTimeMillis() - reloadTimestamp));
				cache.putAll(result);
				timestamp = reloadTimestamp;
				scheduleReload(reloadPeriod);
				callback.setComplete();
			}
		});
	}

	@Override
	public void stop(CompletionCallback callback) {
		scheduledRunnable.cancel();
		callback.setComplete();
	}

	@JmxOperation
	public void reload() {
		doReload();
	}

	@JmxAttribute
	public long getReloadPeriod() {
		return reloadPeriod;
	}

	@JmxAttribute
	public void setReloadPeriod(long reloadPeriod) {
		this.reloadPeriod = reloadPeriod;
	}

	@JmxAttribute
	public long getRetryPeriod() {
		return retryPeriod;
	}

	@JmxAttribute
	public void setRetryPeriod(long retryPeriod) {
		this.retryPeriod = retryPeriod;
	}

	@JmxAttribute
	public int getReloads() {
		return reloads;
	}

	@JmxAttribute
	public int getReloadErrors() {
		return reloadErrors;
	}

	@JmxAttribute
	public int getResolveErrors() {
		return resolveErrors;
	}

	@JmxAttribute
	public K getLastResolveErrorKey() {
		return lastResolveErrorKey;
	}

	@JmxAttribute
	public ValueStats getReloadTime() {
		return reloadTime;
	}
}
