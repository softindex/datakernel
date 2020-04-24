package io.datakernel.launchers.crdt;

import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.TimestampContainer;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import org.junit.Test;

import java.nio.file.Paths;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class CrdtNodeLauncherTest {
	@Test
	public void testInjector() {
		new CrdtNodeLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
				return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {
					@Provides
					CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
						return new CrdtDescriptor<>(
								TimestampContainer.createCrdtFunction(Integer::max),
								new CrdtDataSerializer<>(UTF8_SERIALIZER,
										TimestampContainer.createSerializer(INT_SERIALIZER)),
								STRING_CODEC,
								tuple(TimestampContainer::new,
										TimestampContainer::getTimestamp, LONG_CODEC,
										TimestampContainer::getState, INT_CODEC));
					}

					@Provides
					FsClient fsClient() {
						return LocalFsClient.create(Eventloop.create(), newSingleThreadExecutor(), Paths.get(""));
					}
				};
			}
		}.testInjector();
	}
}
