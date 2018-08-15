package io.datakernel.loader;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;
import io.datakernel.loader.cache.Cache;

class CachedStaticLoader implements StaticLoader {
    private static final byte[] BYTES_ERROR = new byte[]{};

    private final StaticLoader resourceLoader;
    private final Cache cache;

    public CachedStaticLoader(StaticLoader resourceLoader, Cache cache) {
        this.resourceLoader = resourceLoader;
        this.cache = cache;
    }

    @Override
    public Stage<ByteBuf> getResource(String name) {
        byte[] bytes = cache.get(name);

        if (bytes == null) {
	        SettableStage<ByteBuf> stage = new SettableStage<>();
            resourceLoader.getResource(name)
                    .whenComplete((byteBuf, throwable) -> {
                        cache.put(name, throwable == null ? byteBuf.asArray() : BYTES_ERROR);
                        stage.set(byteBuf, throwable);
                    });
            return stage;
        } else {
            if (bytes == BYTES_ERROR) {
                return Stage.ofException(HttpException.notFound404());
            } else {
                return Stage.of(ByteBuf.wrapForReading(bytes));
            }
        }
    }
}
