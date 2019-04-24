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

public final class GlobalPmAdapter<T> implements PmClient<T> {
	private final GlobalPmDriver<T> driver;
	private final PrivKey privKey;
	private final PubKey pubKey;
	private final KeyPair keys;

	public GlobalPmAdapter(GlobalPmDriver<T> driver, PrivKey privKey, PubKey pubKey) {
		this.driver = driver;
		this.privKey = privKey;
		this.pubKey = pubKey;

		keys = new KeyPair(privKey, pubKey);
	}

	@Override
	public Promise<Void> send(PubKey receiver, long timestamp, T payload) {
		return driver.send(privKey, receiver, Message.of(timestamp, pubKey, payload));
	}

	@Override
	public Promise<ChannelConsumer<Message<T>>> multisend(PubKey receiver) {
		return driver.multisend(privKey, receiver);
	}

	@Override
	public Promise<@Nullable Message<T>> poll() {
		return driver.poll(keys);
	}

	@Override
	public Promise<ChannelSupplier<Message<T>>> multipoll() {
		return driver.multipoll(keys);
	}

	@Override
	public Promise<Void> drop(long id) {
		return driver.drop(keys, id);
	}

	@Override
	public Promise<ChannelConsumer<Long>> multidrop() {
		return driver.multidrop(keys);
	}
}
