package io.global.globalfs;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.server.LocalDiscoveryService;
import io.global.globalfs.server.http.DiscoveryServlet;
import io.global.globalfs.server.http.RemoteDiscoveryService;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.util.CollectionUtils.set;

public class DiscoveryHttpTest {

	@Test
	public void test() throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		DiscoveryService serverService = new LocalDiscoveryService();
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, DiscoveryServlet.wrap(serverService)).withListenPort(8080);
		server.listen();

		DiscoveryService clientService = new RemoteDiscoveryService(AsyncHttpClient.create(eventloop), "127.0.0.1:8080");

		KeyPair keys1 = KeyPair.generate();
		KeyPair keys2 = KeyPair.generate();

		InetAddress localhost = InetAddress.getLocalHost();

		clientService.announce(keys1, AnnounceData.of(123, keys1.getPubKey(), set(new RawServerId(new InetSocketAddress(localhost, 123)))))
				.thenCompose($ -> clientService.announce(keys2, AnnounceData.of(124, keys1.getPubKey(), set(new RawServerId(new InetSocketAddress(localhost, 124))))))
				.thenCompose($ -> clientService.findServers(keys1.getPubKey()))
				.whenResult(System.out::println)
				.thenCompose($ -> clientService.findServers(keys2.getPubKey()))
				.whenResult(System.out::println)
				.whenComplete(($, e) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();
	}
}
