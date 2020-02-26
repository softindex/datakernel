package io.datakernel.launchers.crdt;

import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.crdt.primitives.LWWObject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import org.junit.Test;

import java.nio.file.Paths;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public class CrdtNodeLauncherTest {
	@Test
	public void testInjector() {
		new CrdtNodeLauncher<String, Integer>() {
			@Override
			protected CrdtNodeLogicModule<String, Integer> getBusinessLogicModule() {
				return new CrdtNodeLogicModule<String, Integer>() {
					@Provides
					CrdtDescriptor<String, Integer> descriptor() {
						return new CrdtDescriptor<>(Integer::max, new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER), STRING_CODEC, INT_CODEC);
					}

					@Provides
					FsClient fsClient() {
						return LocalFsClient.create(Eventloop.create(), Paths.get(""));
					}
				};
			}
		}.testInjector();
	}
}
