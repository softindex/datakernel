package io.datakernel.launchers.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Collection;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static java.util.Collections.singletonList;

public class CrdtNodeLauncherTest {
	@Test
	public void testInjector() {
		new CrdtNodeLauncher<String, Integer>() {
			@Override
			protected CrdtNodeLogicModule<String, Integer> getLogicModule() {
				return new CrdtNodeLogicModule<String, Integer>() {};
			}

			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(
						new AbstractModule() {
							@Provides
							CrdtDescriptor<String, Integer> provideDescriptor() {
								return new CrdtDescriptor<>(Math::max, new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER), STRING_CODEC, INT_CODEC);
							}

							@Provides
							FsClient provideFsClient() {
								return LocalFsClient.create(Eventloop.create(), Paths.get(""));
							}
						}
				);

			}
		}.testInjector();
	}
}
