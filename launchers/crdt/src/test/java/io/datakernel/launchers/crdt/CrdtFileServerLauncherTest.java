package io.datakernel.launchers.crdt;

import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.TimestampContainer;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import org.junit.Test;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;

public class CrdtFileServerLauncherTest {
	@Test
	public void testInjector() {
		new CrdtFileServerLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtFileServerLogicModule<String, TimestampContainer<Integer>> getLogicModule() {
				return new CrdtFileServerLogicModule<String, TimestampContainer<Integer>>() {};
			}

			@Override
			protected Module getBusinessLogicModule() {
				return new AbstractModule() {
					@Provides
					CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
						return new CrdtDescriptor<>(TimestampContainer.createCrdtFunction(Integer::max),
								new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)), STRING_CODEC,
								tuple(TimestampContainer::new, TimestampContainer::getTimestamp, LONG_CODEC, TimestampContainer::getState, INT_CODEC));
					}
				};
			}
		}.testInjector();
	}
}
