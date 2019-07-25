package io.global.ot;

import io.datakernel.async.RetryPolicy;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.name.ChangeName;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.shared.SharedRepo;
import io.global.ot.shared.SharedReposOperation;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.Utils.PUB_KEY_HEX_CODEC;

public final class OTUtils {
	private OTUtils() {
		throw new AssertionError();
	}

	public static final RetryPolicy POLL_RETRY_POLICY = RetryPolicy.exponentialBackoff(1000, 1000 * 60);

	public static final StructuredCodec<ChangeName> CHANGE_NAME_CODEC = object(ChangeName::new,
			"prev", ChangeName::getPrev, STRING_CODEC,
			"next", ChangeName::getNext, STRING_CODEC,
			"timestamp", ChangeName::getTimestamp, LONG_CODEC);

	public static final StructuredCodec<SharedRepo> SHARED_REPO_CODEC = object(SharedRepo::new,
			"id", SharedRepo::getId, STRING_CODEC,
			"participants", SharedRepo::getParticipants, ofSet(PUB_KEY_HEX_CODEC));

	public static final StructuredCodec<SharedReposOperation> SHARED_REPO_OPERATION_CODEC = object(SharedReposOperation::new,
			"shared repo", SharedReposOperation::getSharedRepo, SHARED_REPO_CODEC,
			"remove", SharedReposOperation::isRemove, BOOLEAN_CODEC);

	public static final StructuredCodec<CreateSharedRepo> SHARED_REPO_MESSAGE_CODEC = SHARED_REPO_CODEC
			.transform(CreateSharedRepo::new, CreateSharedRepo::getSharedRepo);

	@SuppressWarnings("unchecked")
	public static <K, V> StructuredCodec<MapOperation<K, V>> getMapOperationCodec(
			StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		StructuredCodec<SetValue<V>> setValueCodec = StructuredCodecs.object(SetValue::set,
				"prev", SetValue::getPrev, valueCodec.nullable(),
				"next", SetValue::getNext, valueCodec.nullable()
		);
		return ofMap(keyCodec, setValueCodec)
				.transform(MapOperation::of, MapOperation::getOperations);
	}
}
