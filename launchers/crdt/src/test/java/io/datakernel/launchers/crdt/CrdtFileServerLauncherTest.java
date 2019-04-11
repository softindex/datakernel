package io.datakernel.launchers.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.TimestampContainer;
import org.junit.Test;

import java.util.Collection;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static java.util.Collections.singletonList;

public class CrdtFileServerLauncherTest {
	@Test
	public void testInjector() {
		new CrdtFileServerLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtFileServerLogicModule<String, TimestampContainer<Integer>> getLogicModule() {
				return new CrdtFileServerLogicModule<String, TimestampContainer<Integer>>() {};
			}

			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(
						new AbstractModule() {
							@Provides
							CrdtDescriptor<String, TimestampContainer<Integer>> provideDescriptor() {
								return new CrdtDescriptor<>(TimestampContainer.createCrdtFunction(Integer::max),
										new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)), STRING_CODEC,
										tuple(TimestampContainer::new, TimestampContainer::getTimestamp, LONG_CODEC, TimestampContainer::getState, INT_CODEC));
							}
						}
				);
			}
		}.testInjector();
	}
}
