package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.Tuple2;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.RawKvItem;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;
import io.global.pm.util.BinaryDataFormats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import static io.datakernel.codec.StructuredCodecs.LONG64_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.kv.util.BinaryDataFormats.RAW_KV_ITEM_CODEC;

public final class GlobalPmDriver<T> {
	public static final String MESSAGING_TABLE_PREFIX = ApplicationSettings.getString(GlobalPmDriver.class, "messagingTablePrefix", "pm");
	public static final StacklessException INVALID_SIGNATURE = new StacklessException("Received a message with invalid signature");

	private final GlobalKvNode kvNode;
	private final StructuredCodec<Tuple2<PubKey, T>> codec;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public GlobalPmDriver(GlobalKvNode kvNode, StructuredCodec<T> payloadCodec) {
		this.kvNode = kvNode;
		codec = tuple(Tuple2::new, Tuple2::getValue1, BinaryDataFormats.PUB_KEY_CODEC, Tuple2::getValue2, payloadCodec);
	}

	private SignedData<RawKvItem> encrypt(PrivKey sender, PubKey receiver, Message<T> message) {
		byte[] key = encodeAsArray(LONG64_CODEC, message.getId());
		byte[] value = encodeAsArray(codec, new Tuple2<>(message.getSender(), message.getPayload()));
		byte[] encryptedValue = receiver.encrypt(value);
		RawKvItem kvItem = RawKvItem.of(key, encryptedValue, message.getTimestamp());
		return SignedData.sign(RAW_KV_ITEM_CODEC, kvItem, sender);
	}

	private SignedData<RawKvItem> encryptTombstone(PrivKey sender, long id) {
		byte[] key = encodeAsArray(LONG64_CODEC, id);
		RawKvItem kvItem = RawKvItem.tombstone(key, now.currentTimeMillis());
		return SignedData.sign(RAW_KV_ITEM_CODEC, kvItem, sender);
	}

	private Promise<Message<T>> decrypt(PrivKey receiver, SignedData<RawKvItem> signedKvItem) {
		RawKvItem kvItem = signedKvItem.getValue();
		try {
			Tuple2<PubKey, T> data = decode(codec, receiver.decrypt(kvItem.getValue()));
			PubKey sender = data.getValue1();
			return signedKvItem.verify(sender) ?
					Promise.of(Message.parse(decode(LONG64_CODEC, kvItem.getKey()), kvItem.getTimestamp(), sender, data.getValue2())) :
					Promise.ofException(INVALID_SIGNATURE);
		} catch (ParseException | CryptoException e) {
			return Promise.ofException(e);
		}
	}

	public Promise<Void> send(PrivKey sender, PubKey receiver, String mailBox, Message<T> message) {
		return kvNode.put(receiver, getTable(mailBox), encrypt(sender, receiver, message));
	}

	public Promise<ChannelConsumer<Message<T>>> multisend(PrivKey sender, PubKey receiver, String mailBox) {
		return kvNode.upload(receiver, getTable(mailBox))
				.map(consumer -> consumer.map(message -> encrypt(sender, receiver, message)));
	}

	public Promise<@Nullable Message<T>> poll(KeyPair keys, String mailBox) {
		return kvNode.download(keys.getPubKey(), getTable(mailBox))
				.then(supplier -> supplier
						.filter(signedData -> !signedData.getValue().isTombstone())
						.get())
				.then(signedKvItem -> signedKvItem != null ? decrypt(keys.getPrivKey(), signedKvItem) : Promise.of(null));
	}

	public Promise<ChannelSupplier<Message<T>>> multipoll(KeyPair keys, String mailBox) {
		return kvNode.download(keys.getPubKey(), getTable(mailBox))
				.map(supplier -> supplier
						.filter(signedData -> !signedData.getValue().isTombstone())
						.mapAsync(signedData -> decrypt(keys.getPrivKey(), signedData)));
	}

	public Promise<Void> drop(KeyPair keys, String mailBox, long id) {
		return kvNode.put(keys.getPubKey(), getTable(mailBox), encryptTombstone(keys.getPrivKey(), id));
	}

	public Promise<ChannelConsumer<Long>> multidrop(KeyPair keys, String mailBox) {
		return kvNode.upload(keys.getPubKey(), getTable(mailBox))
				.map(consumer -> consumer.map(id -> encryptTombstone(keys.getPrivKey(), id)));
	}

	public PmClient<T> adapt(PrivKey privKey, PubKey pubKey) {
		return new GlobalPmAdapter<>(this, privKey, pubKey);
	}

	public PmClient<T> adapt(KeyPair keys) {
		return new GlobalPmAdapter<>(this, keys.getPrivKey(), keys.getPubKey());
	}

	@NotNull
	private String getTable(String mailBox) {
		return MESSAGING_TABLE_PREFIX + "/" + mailBox;
	}
}
