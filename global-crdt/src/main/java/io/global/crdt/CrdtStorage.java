package io.global.crdt;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

public interface CrdtStorage {

	Promise<StreamConsumer<SignedData<RawCrdtData>>> upload();

	Promise<StreamSupplier<SignedData<RawCrdtData>>> download(long revision);

	default Promise<StreamSupplier<SignedData<RawCrdtData>>> download() {
		return download(0);
	}

	Promise<StreamConsumer<SignedData<byte[]>>> remove();

	Promise<@Nullable SignedData<RawCrdtData>> get(byte[] key);

	default Promise<Void> put(SignedData<RawCrdtData> item) {
		return StreamSupplier.of(item).streamTo(StreamConsumer.ofPromise(upload()));
	}

	default Promise<Void> remove(SignedData<byte[]> key) {
		return StreamSupplier.of(key).streamTo(StreamConsumer.ofPromise(remove()));
	}
}
