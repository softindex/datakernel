package io.global.pm;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.RawKvItem;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import java.util.function.Supplier;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.Utils.PUB_KEY_HEX_CODEC;
import static io.global.kv.util.BinaryDataFormats.RAW_KV_ITEM_CODEC;

public final class Messenger<K, V> {
	public static final String MESSAGING_TABLE_PREFIX = ApplicationSettings.getString(Messenger.class, "messagingTablePrefix", "pm");
	public static final StacklessException INVALID_SIGNATURE = new StacklessException("Received a message with invalid signature");

	private final GlobalKvNode kvNode;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<V> valueCodec;
	private final StructuredCodec<Tuple2<PubKey, V>> kvValueCodec;
	private final Supplier<K> idGenerator;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private Messenger(GlobalKvNode kvNode, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec, Supplier<K> idGenerator) {
		this.kvNode = kvNode;
		this.keyCodec = keyCodec;
		this.valueCodec = valueCodec;
		this.kvValueCodec = tuple(Tuple2::new, Tuple2::getValue1, PUB_KEY_HEX_CODEC, Tuple2::getValue2, valueCodec);
		this.idGenerator = idGenerator;
	}

	public static <K, V> Messenger<K, V> create(GlobalKvNode kvNode, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec, Supplier<K> idGenerator) {
		return new Messenger<>(kvNode, keyCodec, valueCodec, idGenerator);
	}

	public StructuredCodec<K> getKeyCodec() {
		return keyCodec;
	}

	public StructuredCodec<V> getValueCodec() {
		return valueCodec;
	}

	public Promise<K> send(KeyPair sender, PubKey receiver, String mailBox, V payload) {
		K id = idGenerator.get();
		Message<K, V> message = Message.now(id, sender.getPubKey(), payload);
		return kvNode.put(receiver, getTable(mailBox), encrypt(sender.getPrivKey(), receiver, message))
				.map($ -> id);
	}

	public Promise<@Nullable Message<K, V>> poll(KeyPair receiver, String mailBox) {
		return kvNode.download(receiver.getPubKey(), getTable(mailBox))
				.then(supplier -> supplier
						.filter(signedData -> !signedData.getValue().isTombstone())
						.get())
				.then(signedKvItem -> signedKvItem != null ? decrypt(receiver.getPrivKey(), signedKvItem) : Promise.of(null));
	}

	public Promise<Void> drop(KeyPair keys, String mailBox, K id) {
		return kvNode.put(keys.getPubKey(), getTable(mailBox), encryptTombstone(keys.getPrivKey(), id));
	}

	private SignedData<RawKvItem> encrypt(PrivKey sender, PubKey receiver, Message<K, V> message) {
		byte[] key = encodeAsArray(keyCodec, message.getId());
		byte[] value = encodeAsArray(kvValueCodec, new Tuple2<>(message.getSender(), message.getPayload()));
		byte[] encryptedValue = receiver.encrypt(value);
		RawKvItem kvItem = RawKvItem.of(key, encryptedValue, message.getTimestamp());
		return SignedData.sign(RAW_KV_ITEM_CODEC, kvItem, sender);
	}

	private SignedData<RawKvItem> encryptTombstone(PrivKey sender, K id) {
		byte[] key = encodeAsArray(keyCodec, id);
		RawKvItem kvItem = RawKvItem.tombstone(key, now.currentTimeMillis());
		return SignedData.sign(RAW_KV_ITEM_CODEC, kvItem, sender);
	}

	private Promise<Message<K, V>> decrypt(PrivKey receiver, SignedData<RawKvItem> signedKvItem) {
		RawKvItem kvItem = signedKvItem.getValue();
		try {
			Tuple2<PubKey, V> data = decode(kvValueCodec, receiver.decrypt(kvItem.getValue()));
			PubKey sender = data.getValue1();
			return signedKvItem.verify(sender) ?
					Promise.of(Message.parse(decode(keyCodec, kvItem.getKey()), kvItem.getTimestamp(), sender, data.getValue2())) :
					Promise.ofException(INVALID_SIGNATURE);
		} catch (ParseException | CryptoException e) {
			return Promise.ofException(e);
		}
	}

	private String getTable(String mailBox) {
		return MESSAGING_TABLE_PREFIX + "/" + mailBox;
	}
}
