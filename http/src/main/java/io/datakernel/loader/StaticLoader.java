package io.datakernel.loader;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;

public interface StaticLoader {

    Stage<ByteBuf> getResource(String name);

}