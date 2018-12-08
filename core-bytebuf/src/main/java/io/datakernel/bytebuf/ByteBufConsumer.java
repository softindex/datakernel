package io.datakernel.bytebuf;

public interface ByteBufConsumer {
	void accept(ByteBuf buf);
}
