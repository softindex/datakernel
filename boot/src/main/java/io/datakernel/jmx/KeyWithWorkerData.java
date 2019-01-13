package io.datakernel.jmx;

import com.google.inject.Key;
import io.datakernel.worker.WorkerPool;
import org.jetbrains.annotations.Nullable;

public final class KeyWithWorkerData {
	private final Key<?> key;

	@Nullable
	private final WorkerPool pool;
	private final int workerId;

	public KeyWithWorkerData(Key<?> key) {
		this(key, null, -1);
	}

	public KeyWithWorkerData(Key<?> key, @Nullable WorkerPool pool, int workerId) {
		this.key = key;
		this.pool = pool;
		this.workerId = workerId;
	}

	public Key<?> getKey() {
		return key;
	}

	@Nullable
	public WorkerPool getPool() {
		return pool;
	}

	public int getWorkerId() {
		return workerId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KeyWithWorkerData that = (KeyWithWorkerData) o;

		return workerId == that.workerId
				&& key.equals(that.key)
				&& (pool != null ? pool.equals(that.pool) : that.pool == null);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * key.hashCode() + (pool != null ? pool.hashCode() : 0)) + workerId;
	}

	@Override
	public String toString() {
		return "KeyWithWorkerData{key=" + key + ", pool=" + pool + ", workerId=" + workerId + '}';
	}
}
