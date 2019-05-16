package io.global.fs.app.container;

import io.datakernel.async.RetryPolicy;
import io.datakernel.codec.StructuredCodec;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.fs.app.container.message.CreateSharedDirMessage;
import io.global.fs.app.container.message.SharedDirMessage;

import java.util.Set;
import java.util.regex.Pattern;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.pm.util.HttpDataFormats.PUB_KEY_HEX_CODEC;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<SharedSimKey> SHARED_SIM_KEY_CODEC = REGISTRY.get(SharedSimKey.class);
	public static final StructuredCodec<Set<PubKey>> PARTICIPANTS_CODEC = ofSet(PUB_KEY_HEX_CODEC);
	public static final StructuredCodec<SharedDirMetadata> SHARED_DIR_METADATA_CODEC = object(SharedDirMetadata::new,
			"dir id", SharedDirMetadata::getDirId, STRING_CODEC,
			"dir name", SharedDirMetadata::getDirName, STRING_CODEC,
			"participants", SharedDirMetadata::getParticipants, PARTICIPANTS_CODEC,
			"key", SharedDirMetadata::getSharedSimKey, SHARED_SIM_KEY_CODEC);
	public static final StructuredCodec<SharedDirMessage> PAYLOAD_CODEC = SHARED_DIR_METADATA_CODEC
			.transform(CreateSharedDirMessage::new, SharedDirMessage::getDirMetadata);

	public static final RetryPolicy RETRY_POLICY = RetryPolicy.exponentialBackoff(1000, 30_000);
	public static final String HIDDEN_FILE_PREFIX = ".~#!HIDDEN!#~.";
	public static final String SHARED_DIR_PATH = "_shared";
	public static final String META_DIR_PATH = SHARED_DIR_PATH + '/' + HIDDEN_FILE_PREFIX + "_meta";
	public static final String META_KEY_PATH = META_DIR_PATH + "/ssk.dat";
	public static final Pattern FILENAME_MATCHER = Pattern.compile(".*/(.*)\\..*");
	public static final String FS_SHARED_DIR_MAILBOX = "FS shared dirs";

}
