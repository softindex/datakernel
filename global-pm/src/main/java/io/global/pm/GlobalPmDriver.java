package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.util.Tuple2;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;
import io.global.pm.api.RawMessage;
import io.global.pm.util.BinaryDataFormats;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.pm.util.BinaryDataFormats.RAW_MESSAGE_CODEC;

public final class GlobalPmDriver<T> {
	private static final StacklessException INVALID_SIGNATURE = new StacklessException("Received a message with invalid signature");
	private static final Supplier<Long> DEFAULT_ID_GENERATOR = ThreadLocalRandom.current()::nextLong;

	private final GlobalPmNode node;
	private final StructuredCodec<T> payloadCodec;
	private final StructuredCodec<Tuple2<PubKey, T>> tupleCodec;

	private Supplier<Long> idGenerator = DEFAULT_ID_GENERATOR;

	private GlobalPmDriver(GlobalPmNode node, StructuredCodec<T> payloadCodec) {
		this.node = node;
		this.payloadCodec = payloadCodec;
		tupleCodec = tuple(Tuple2::new, Tuple2::getValue1, BinaryDataFormats.PUB_KEY_CODEC, Tuple2::getValue2, payloadCodec);
	}

	public static <T> GlobalPmDriver<T> create(GlobalPmNode node, StructuredCodec<T> payloadCodec) {
		return new GlobalPmDriver<>(node, payloadCodec);
	}

	public GlobalPmDriver<T> withIdGenerator(Supplier<Long> idGenerator) {
		this.idGenerator = idGenerator;
		return this;
	}

	public StructuredCodec<T> getPayloadCodec() {
		return payloadCodec;
	}

	private SignedData<RawMessage> encrypt(PrivKey sender, PubKey receiver, Long id, Long timestamp, @Nullable T payload) {
		byte[] encrypted = payload != null ?
				receiver.encrypt(BinaryUtils.encodeAsArray(tupleCodec, new Tuple2<>(sender.computePubKey(), payload))) :
				null;
		RawMessage msg = RawMessage.of(id, timestamp, encrypted);
		return SignedData.sign(RAW_MESSAGE_CODEC, msg, sender);
	}

	private Promise<Message<T>> decrypt(PrivKey receiver, SignedData<RawMessage> signedRawMessage) {
		RawMessage raw = signedRawMessage.getValue();
		try {
			Tuple2<PubKey, T> data = BinaryUtils.decode(tupleCodec, receiver.decrypt(raw.getEncrypted()));
			PubKey sender = data.getValue1();
			return signedRawMessage.verify(sender) ?
					Promise.of(Message.parse(raw.getId(), raw.getTimestamp(), sender, data.getValue2())) :
					Promise.ofException(INVALID_SIGNATURE);
		} catch (ParseException | CryptoException e) {
			return Promise.ofException(e);
		}
	}

	public Promise<Void> send(PrivKey sender, PubKey receiver, String mailBox, T payload) {
		long id = idGenerator.get();
		long timestamp = System.currentTimeMillis();
		return node.send(receiver, mailBox, encrypt(sender, receiver, id, timestamp, payload));
	}

	public Promise<ChannelConsumer<T>> multisend(PrivKey sender, PubKey receiver, String mailBox) {
		return node.upload(receiver, mailBox)
				.map(consumer -> consumer.map(payload -> {
					long id = idGenerator.get();
					long timestamp = System.currentTimeMillis();
					return encrypt(sender, receiver, id, timestamp, payload);
				}));
	}

	public Promise<@Nullable Message<T>> poll(KeyPair keys, String mailBox) {
		return node.poll(keys.getPubKey(), mailBox)
				.then(signedRawMessage -> signedRawMessage != null ? decrypt(keys.getPrivKey(), signedRawMessage) : Promise.of(null));
	}

	public Promise<ChannelSupplier<Message<T>>> multipoll(KeyPair keys, String mailBox, long timestamp) {
		PrivKey privKey = keys.getPrivKey();
		return node.download(keys.getPubKey(), mailBox, timestamp)
				.map(supplier -> supplier
						.filter(signedData -> signedData.getValue().isMessage())
						.mapAsync(signedRawMessage -> decrypt(privKey, signedRawMessage)));
	}

	public Promise<ChannelSupplier<Message<T>>> multipoll(KeyPair keys, String mailBox) {
		return multipoll(keys, mailBox, 0);
	}

	public Promise<List<Message<T>>> batchpoll(KeyPair keys, String mailBox, long timestamp){
		return multipoll(keys, mailBox, timestamp).then(ChannelSupplier::toList);
	}

	public Promise<List<Message<T>>> batchpoll(KeyPair keys, String mailBox){
		return batchpoll(keys, mailBox, 0);
	}

	public Promise<Void> drop(KeyPair keys, String mailBox, long id) {
		long timestamp = System.currentTimeMillis();
		return node.send(keys.getPubKey(), mailBox, encrypt(keys.getPrivKey(), keys.getPubKey(), id, timestamp, null));
	}

	public Promise<ChannelConsumer<Long>> multidrop(KeyPair keys, String mailBox) {
		return node.upload(keys.getPubKey(), mailBox)
				.map(consumer -> consumer.map(id -> encrypt(keys.getPrivKey(), keys.getPubKey(), id, System.currentTimeMillis(), null)));
	}

	public PmClient<T> adapt(PrivKey privKey) {
		return new GlobalPmAdapter<>(this, privKey);
	}

	public PmClient<T> adapt(KeyPair keys) {
		return new GlobalPmAdapter<>(this, keys.getPrivKey());
	}
}
