package io.datakernel.launchers.crdt;

import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.TimestampContainer;
import io.datakernel.di.annotation.Provides;
import org.junit.Test;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public class CrdtFileServerLauncherTest {
	@Test
	public void testInjector() {
		new CrdtFileServerLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtFileServerLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
				return new CrdtFileServerLogicModule<String, TimestampContainer<Integer>>() {};
			}

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
		}.testInjector();
	}
}
