package io.global.fs.app.container;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.global.common.PrivKey;
import io.global.fs.app.container.message.SharedDirMessage;
import io.global.fs.local.GlobalFsDriver;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.GlobalPmNode;

import static io.global.fs.app.container.Utils.PAYLOAD_CODEC;

public final class UserContainerModule extends AbstractModule {

	@Provides
	@Singleton
	FsUserContainerHolder provideContainerHolder(Eventloop eventloop, GlobalFsDriver fsDriver, GlobalPmDriver<SharedDirMessage> pmDriver) {
		return FsUserContainerHolder.create(eventloop, fsDriver, pmDriver);
	}

	@Provides
	@Singleton
	GlobalPmDriver<SharedDirMessage> providePmDriver(GlobalPmNode pmNode) {
		return new GlobalPmDriver<>(pmNode, PAYLOAD_CODEC);
	}

	@Provides
	@Singleton
	FsMessagingServlet providesMessagingServlet(FsUserContainerHolder containerHolder) {
		return FsMessagingServlet.create(containerHolder);
	}

	@Provides
	@Singleton
	@Named("Service")
	AsyncServlet provideServiceEnsuringServlet(FsUserContainerHolder containerHolder, @Named("App") AsyncServlet servlet) {
		return request -> {
			try {
				String key = request.getCookieOrNull("Key");
				PrivKey privKey = key == null ? null : PrivKey.fromString(key);
				return (key == null ? Promise.complete() : containerHolder.ensureUserContainer(privKey))
						.then($ -> servlet.serve(request));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}
}
