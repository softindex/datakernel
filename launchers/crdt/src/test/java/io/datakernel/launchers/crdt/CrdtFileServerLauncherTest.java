package io.datakernel.launchers.crdt;

import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.crdt.primitives.LWWObject;
import io.datakernel.di.annotation.Provides;
import org.junit.Test;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public class CrdtFileServerLauncherTest {
	@Test
	public void testInjector() {
		new CrdtFileServerLauncher<String, Integer>() {
			@Override
			protected CrdtFileServerLogicModule<String, Integer> getBusinessLogicModule() {
				return new CrdtFileServerLogicModule<String, Integer>() {};
			}

			@Provides
			CrdtDescriptor<String, Integer> descriptor() {
				return new CrdtDescriptor<>(Integer::max, new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER), STRING_CODEC, INT_CODEC);
			}
		}.testInjector();
	}
}
