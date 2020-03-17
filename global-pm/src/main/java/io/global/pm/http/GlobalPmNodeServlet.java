package io.global.pm.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.binary.BinaryUtils.encodeWithSizePrefix;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.BinaryDataFormats.SIGNED_RAW_MSG_CODEC;
import static io.global.pm.util.BinaryDataFormats.SIGNED_RAW_MSG_PARSER;

public class GlobalPmNodeServlet {
	private static final Duration KEEP_ALIVE_INTERVAL = ApplicationSettings.getDuration(GlobalPmNodeServlet.class, "keepAlive", Duration.ofSeconds(10));

	public static final StructuredCodec<Set<String>> STRING_SET_CODEC = StructuredCodecs.ofSet(STRING_CODEC);

	public static RoutingServlet create(GlobalPmNode node) {
		return RoutingServlet.create()
				.map(POST, "/" + SEND + "/:space/:mailbox", loadBody()
						.serve(request -> {
							try {
								String spacePathParameter = request.getPathParameter("space");
								String mailBox = request.getPathParameter("mailbox");
								PubKey space = PubKey.fromString(spacePathParameter);
								SignedData<RawMessage> message = BinaryUtils.decode(SIGNED_RAW_MSG_CODEC, request.getBody().getArray());
								return node.send(space, mailBox, message)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})
				)
				.map(GET, "/" + POLL + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						return node.poll(space, mailBox)
								.map(message -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(SIGNED_RAW_MSG_CODEC.nullable(), message)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + LIST + "/:space", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						return node.list(space)
								.map(mailBoxes -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(STRING_SET_CODEC, mailBoxes)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + DOWNLOAD + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						long timestamp;
						try {
							String timestampParam = request.getQueryParameter("timestamp");
							timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return node.download(space, mailBox, timestamp)
								.map(supplier -> HttpResponse.ok200()
										.withBodyStream(supplier.map(signedMessage -> encodeWithSizePrefix(SIGNED_RAW_MSG_CODEC, signedMessage))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(POST, "/" + UPLOAD + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return node.upload(space, mailBox)
								.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(SIGNED_RAW_MSG_PARSER)
										.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + STREAM + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						long timestamp;
						try {
							String timestampParam = request.getQueryParameter("timestamp");
							timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return node.stream(space, mailBox, timestamp)
								.map(supplier -> HttpResponse.ok200()
										.withBodyStream(ChannelSupplier.ofConsumer(channelConsumer ->
												supplier.map(GlobalPmNodeServlet::encodeForKeepAlive)
														.streamTo(new KeptAliveChannelConsumer(channelConsumer)), new ChannelZeroBuffer<>())));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	private static ByteBuf encodeForKeepAlive(SignedData<RawMessage> signedMessage) {
		ByteBuf prefix = ByteBuf.wrapForReading(new byte[]{1});
		ByteBuf encoded = encodeWithSizePrefix(SIGNED_RAW_MSG_CODEC, signedMessage);
		return ByteBufPool.append(prefix, encoded);
	}

	private static class KeptAliveChannelConsumer extends AbstractChannelConsumer<ByteBuf> {

		private final ChannelConsumer<ByteBuf> channelConsumer;
		private ScheduledRunnable scheduledKeepAlive;
		private Promise<Void> keepAlivePromise = Promise.complete();

		public KeptAliveChannelConsumer(ChannelConsumer<ByteBuf> channelConsumer) {
			super(channelConsumer);
			this.channelConsumer = channelConsumer;
			resetKeepAlive();
		}

		void resetKeepAlive() {
			if (scheduledKeepAlive != null) scheduledKeepAlive.cancel();
			scheduledKeepAlive = getCurrentEventloop()
					.scheduleBackground(System.currentTimeMillis() + KEEP_ALIVE_INTERVAL.toMillis(),
							() -> {
								if (keepAlivePromise.isComplete()) {
									keepAlivePromise = channelConsumer.accept(ByteBuf.wrapForReading(new byte[]{0}));
								} else {
									keepAlivePromise = keepAlivePromise.then($ -> channelConsumer.accept(ByteBuf.wrapForReading(new byte[]{0})));
								}
								resetKeepAlive();
							});
		}

		@Override
		protected Promise<Void> doAccept(@Nullable ByteBuf value) {
			resetKeepAlive();
			return keepAlivePromise
					.then($ -> channelConsumer.accept(value));
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			scheduledKeepAlive.cancel();
			channelConsumer.close(e);
		}
	}
}
