package io.global.pn;

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
import io.global.pn.api.GlobalPmNode;
import io.global.pn.api.Message;
import io.global.pn.api.PmClient;
import io.global.pn.api.RawMessage;
import io.global.pn.util.BinaryDataFormats;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import static io.datakernel.codec.StructuredCodecs.LONG64_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;

public final class GlobalPmDriver<T> {
	private static final StacklessException INVALID_SIGNATURE = new StacklessException("Received a message with invalid signature");

	private final GlobalPmNode node;
	private final StructuredCodec<Tuple2<PubKey, T>> codec;

	public GlobalPmDriver(GlobalPmNode node, StructuredCodec<T> payloadCodec) {
		this.node = node;

		codec = tuple(Tuple2::new, Tuple2::getValue1, BinaryDataFormats.PUB_KEY_CODEC, Tuple2::getValue2, payloadCodec);
	}

	private SignedData<RawMessage> encrypt(PrivKey sender, PubKey receiver, Message<T> message) {
		byte[] encrypted = BinaryUtils.encodeAsArray(codec, new Tuple2<>(message.getSender(), message.getPayload()));
		RawMessage msg = new RawMessage(message.getId(), message.getTimestamp(), receiver.encrypt(encrypted));
		return SignedData.sign(BinaryDataFormats.RAW_MESSAGE_CODEC, msg, sender);
	}

	private Promise<Message<T>> decrypt(PrivKey receiver, SignedData<RawMessage> signedRawMessage) {
		RawMessage raw = signedRawMessage.getValue();
		try {
			Tuple2<PubKey, T> data = BinaryUtils.decode(codec, receiver.decrypt(raw.getEncrypted()));
			PubKey sender = data.getValue1();
			return signedRawMessage.verify(sender) ?
					Promise.of(Message.parse(raw.getId(), raw.getTimestamp(), sender, data.getValue2())) :
					Promise.ofException(INVALID_SIGNATURE);
		} catch (ParseException | CryptoException e) {
			return Promise.ofException(e);
		}
	}

	public Promise<Void> send(PrivKey sender, PubKey receiver, String mailBox, Message<T> message) {
		return node.send(receiver, mailBox, encrypt(sender, receiver, message));
	}

	public Promise<ChannelConsumer<Message<T>>> multisend(PrivKey sender, PubKey receiver, String mailBox) {
		return node.multisend(receiver, mailBox)
				.map(consumer -> consumer.map(message -> encrypt(sender, receiver, message)));
	}

	public Promise<@Nullable Message<T>> poll(KeyPair keys, String mailBox) {
		return node.poll(keys.getPubKey(), mailBox)
				.then(signedRawMessage -> signedRawMessage != null ? decrypt(keys.getPrivKey(), signedRawMessage) : Promise.of(null));
	}

	public Promise<ChannelSupplier<Message<T>>> multipoll(KeyPair keys, String mailBox) {
		PrivKey privKey = keys.getPrivKey();
		return node.multipoll(keys.getPubKey(), mailBox)
				.map(supplier -> supplier.mapAsync(signedRawMessage -> decrypt(privKey, signedRawMessage)));
	}

	public Promise<Void> drop(KeyPair keys, String mailBox, long id) {
		return node.drop(keys.getPubKey(), mailBox, SignedData.sign(LONG64_CODEC, id, keys.getPrivKey()));
	}

	public Promise<ChannelConsumer<Long>> multidrop(KeyPair keys, String mailBox) {
		PrivKey privKey = keys.getPrivKey();
		return node.multidrop(keys.getPubKey(), mailBox)
				.map(consumer -> consumer.map(id -> SignedData.sign(LONG64_CODEC, id, privKey)));
	}

	public PmClient<T> adapt(PrivKey privKey, PubKey pubKey) {
		return new GlobalPmAdapter<>(this, privKey, pubKey);
	}

	public PmClient<T> adapt(KeyPair keys) {
		return new GlobalPmAdapter<>(this, keys.getPrivKey(), keys.getPubKey());
	}
}
