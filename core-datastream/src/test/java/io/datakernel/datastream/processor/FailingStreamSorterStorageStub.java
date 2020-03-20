package io.datakernel.datastream.processor;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;

import java.util.List;

import static io.datakernel.common.Preconditions.checkNotNull;

public final class FailingStreamSorterStorageStub<T> implements StreamSorterStorage<T> {
	public static final Exception STORAGE_EXCEPTION = new ExpectedException("failing storage");

	private StreamSorterStorage<T> storage;

	boolean failNewPartition;
	boolean failWrite;
	boolean failRead;
	boolean failCleanup;

	private FailingStreamSorterStorageStub(StreamSorterStorage<T> storage) {
		this.storage = storage;
	}

	public static <T> FailingStreamSorterStorageStub<T> create(StreamSorterStorage<T> storage) {
		return new FailingStreamSorterStorageStub<>(storage);
	}

	public static <T> FailingStreamSorterStorageStub<T> create() {
		return new FailingStreamSorterStorageStub<>(null);
	}

	public FailingStreamSorterStorageStub<T> withFailNewPartition(){
		this.failNewPartition = true;
		return this;
	}

	public FailingStreamSorterStorageStub<T> withFailWrite(){
		this.failWrite = true;
		return this;
	}

	public FailingStreamSorterStorageStub<T> withFailRead(){
		this.failRead = true;
		return this;
	}

	public FailingStreamSorterStorageStub<T> withFailCleanup(){
		this.failCleanup = true;
		return this;
	}

	public void setStorage(StreamSorterStorage<T> storage){
		this.storage = storage;
	}

	@Override
	public Promise<Integer> newPartitionId() {
		checkNotNull(storage);
		return failNewPartition ? Promise.ofException(STORAGE_EXCEPTION) : storage.newPartitionId();
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		checkNotNull(storage);
		return failWrite ? Promise.ofException(STORAGE_EXCEPTION) : storage.write(partition);
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		checkNotNull(storage);
		return failRead ? Promise.ofException(STORAGE_EXCEPTION) : storage.read(partition);
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		checkNotNull(storage);
		return failCleanup ? Promise.ofException(STORAGE_EXCEPTION) : storage.cleanup(partitionsToDelete);
	}
}
