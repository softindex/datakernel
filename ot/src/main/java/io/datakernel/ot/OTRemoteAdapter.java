package io.datakernel.ot;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.RunnableWithException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public final class OTRemoteAdapter<K, D> implements OTRemote<K, D> {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final OTRemoteBlocking<K, D> blocking;

	private OTRemoteAdapter(Eventloop eventloop, ExecutorService executor, OTRemoteBlocking<K, D> blocking) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.blocking = blocking;
	}

	public static <K, D> OTRemoteAdapter<K, D> ofBlockingRemote(Eventloop eventloop, ExecutorService executor, OTRemoteBlocking<K, D> blocking) {
		return new OTRemoteAdapter<K, D>(eventloop, executor, blocking);
	}

	@Override
	public void createId(ResultCallback<K> callback) {
		eventloop.callConcurrently(executor, new Callable<K>() {
			@Override
			public K call() throws Exception {
				return blocking.createId();
			}
		}, callback);
	}

	@Override
	public void push(final List<OTCommit<K, D>> commits, CompletionCallback callback) {
		eventloop.runConcurrently(executor, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				blocking.push(commits);
			}
		}, callback);
	}

	@Override
	public void getHeads(ResultCallback<Set<K>> callback) {
		eventloop.callConcurrently(executor, new Callable<Set<K>>() {
			@Override
			public Set<K> call() throws Exception {
				return blocking.getHeads();
			}
		}, callback);
	}

	@Override
	public void getCheckpoint(ResultCallback<K> callback) {
		eventloop.callConcurrently(executor, new Callable<K>() {
			@Override
			public K call() throws Exception {
				return blocking.getCheckpoint();
			}
		}, callback);
	}

	@Override
	public void loadCommit(final K revisionId, ResultCallback<OTCommit<K, D>> callback) {
		eventloop.callConcurrently(executor, new Callable<OTCommit<K, D>>() {
			@Override
			public OTCommit<K, D> call() throws Exception {
				return blocking.loadCommit(revisionId);
			}
		}, callback);
	}

}
