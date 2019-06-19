package io.global.ot;

import io.datakernel.async.RetryPolicy;
import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.exception.ParseException;
import io.global.ot.edit.DeleteOperation;
import io.global.ot.edit.EditOperation;
import io.global.ot.edit.InsertOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.shared.CreateOrDropRepos;
import io.global.ot.shared.RenameRepos;
import io.global.ot.shared.RepoInfo;
import io.global.ot.shared.SharedReposOperation;
import io.global.ot.value.ChangeValue;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.Utils.PUB_KEY_HEX_CODEC;
import static io.global.ot.edit.DeleteOperation.DELETE_CODEC;
import static io.global.ot.edit.InsertOperation.INSERT_CODEC;

public final class OTUtils {
	private OTUtils() {
		throw new AssertionError();
	}

	public static final RetryPolicy POLL_RETRY_POLICY = RetryPolicy.exponentialBackoff(1000, 1000 * 60);

	public static final StructuredCodec<RepoInfo> REPO_INFO_CODEC = object(RepoInfo::new,
			"name", RepoInfo::getName, STRING_CODEC,
			"participants", RepoInfo::getParticipants, ofSet(PUB_KEY_HEX_CODEC),
			"remove", RepoInfo::isRemove, BOOLEAN_CODEC);

	public static final StructuredCodec<CreateSharedRepo> SHARED_REPO_MESSAGE_CODEC = object(CreateSharedRepo::new,
			"id", CreateSharedRepo::getId, STRING_CODEC,
			"name", CreateSharedRepo::getName, STRING_CODEC,
			"participants", CreateSharedRepo::getParticipants, ofSet(PUB_KEY_HEX_CODEC));

	public static final StructuredCodec<CreateOrDropRepos> CREATE_OR_DROP_REPO_CODEC = ofMap(STRING_CODEC, REPO_INFO_CODEC)
			.transform(CreateOrDropRepos::new, CreateOrDropRepos::getRepoInfos);

	public static final StructuredCodec<RenameRepos> RENAME_REPO_CODEC = getMapOperationCodec(STRING_CODEC, STRING_CODEC)
			.transform(RenameRepos::new, RenameRepos::getRenames);

	public static <V> StructuredCodec<SetValue<V>> getSetValueCodec(StructuredCodec<V> valueCodec) {
		return object(SetValue::set,
				"prev", SetValue::getPrev, valueCodec.nullable(),
				"next", SetValue::getNext, valueCodec.nullable());
	}

	public static final StructuredCodec<SharedReposOperation> SHARED_REPOS_OPERATION_CODEC = CodecSubtype.<SharedReposOperation>create()
			.with(CreateOrDropRepos.class, "CreateOrDropRepo", CREATE_OR_DROP_REPO_CODEC)
			.with(RenameRepos.class, "RenameRepo", RENAME_REPO_CODEC)
			.withTagName("type", "value");

	public static <K, V> StructuredCodec<MapOperation<K, V>> getMapOperationCodec(StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return ofMap(keyCodec, getSetValueCodec(valueCodec))
				.transform(MapOperation::of, MapOperation::getOperations);
	}

	public static final StructuredCodec<EditOperation> EDIT_OPERATION_CODEC = StructuredCodec.ofObject(
			in -> {
				in.readKey("type");
				String type = in.readString();
				in.readKey("value");
				switch (type) {
					case "Insert":
						return INSERT_CODEC.decode(in);
					case "Delete":
						return DELETE_CODEC.decode(in);
					default:
						throw new ParseException("Either Insert or Delete is expected");
				}
			}, (out, item) -> {
				out.writeKey("type");
				if (item instanceof InsertOperation) {
					out.writeString("Insert");
					out.writeKey("value", INSERT_CODEC, (InsertOperation) item);
				} else if (item instanceof DeleteOperation) {
					out.writeString("Delete");
					out.writeKey("value", DELETE_CODEC, (DeleteOperation) item);
				} else {
					throw new IllegalArgumentException("Item should be either InsertOperation or DeleteOperation");
				}
			}
	);

	public static <T> StructuredCodec<ChangeValue<T>> ofChangeValue(StructuredCodec<T> underlying) {
		return object(ChangeValue::of,
				"prev", ChangeValue::getPrev, underlying,
				"next", ChangeValue::getNext, underlying,
				"timestamp", ChangeValue::getTimestamp, LONG_CODEC);
	}

	public static CodecRegistry createOTRegistry() {
		return CodecRegistry.createDefault()
				.withGeneric(MapOperation.class, (registry, subCodecs) -> getMapOperationCodec(subCodecs[0], subCodecs[1]))
				.withGeneric(SetValue.class, (registry, subCodecs) -> getSetValueCodec(subCodecs[0]))
				.withGeneric(ChangeValue.class, (registry, subCodecs) -> ofChangeValue(subCodecs[0]));
	}
}
