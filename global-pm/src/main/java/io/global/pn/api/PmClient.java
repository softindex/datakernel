package io.global.pn.api;

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
import io.global.pn.util.BinaryDataFormats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import static io.datakernel.codec.StructuredCodecs.LONG64_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;

public interface PmClient<T> {

	Promise<Void> send(PubKey receiver, long timestamp, T payload);

	Promise<ChannelConsumer<Message<T>>> multisend(PubKey receiver);

	Promise<@Nullable Message<T>> poll();

	Promise<ChannelSupplier<Message<T>>> multipoll();

	Promise<Void> drop(long id);

	Promise<ChannelConsumer<Long>> multidrop();
}
