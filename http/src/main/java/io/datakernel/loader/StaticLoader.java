package io.datakernel.loader;

import io.datakernel.bytebuf.ByteBuf;

import java.util.concurrent.CompletionStage;

public interface StaticLoader {

    CompletionStage<ByteBuf> getResource(String name);

}