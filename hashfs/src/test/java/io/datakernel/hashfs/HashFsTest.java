/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.hashfs;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.protocol.HashFsClientProtocol;
import io.datakernel.hashfs.protocol.gson.HashFsGsonClientProtocol;
import io.datakernel.hashfs.protocol.gson.HashFsGsonServer;
import io.datakernel.hashfs.stub.CurrentServerFileMap;
import io.datakernel.hashfs.stub.SimpleServerStatusNotifier;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HashFsTest {
	private static final Logger logger = LoggerFactory.getLogger(HashFsTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path dirPath;

	private static HashMultimap<Integer, String> getInitServerWithFilesState() {
		HashMultimap<Integer, String> initState = HashMultimap.create();
		initState.put(0, "big_file");
		initState.put(0, "t_dir/t1");
		initState.put(0, "a b");
		initState.put(0, "empty_file");
		initState.put(1, "t_dir/t1");
		initState.put(2, "t_dir/t2");
		initState.put(2, "a b");
		initState.put(2, "empty_file");
		initState.put(3, "big_file");
		initState.put(3, "t_dir/t2");
		initState.put(4, "t_dir/t2");
		initState.put(4, "t_dir/t1");
		initState.put(4, "a b");
		initState.put(4, "big_file");
		initState.put(4, "empty_file");

		return initState;
	}

	private static HashFsImpl createClient(NioEventloop eventloop, List<ServerInfo> serverInfos) {
		return HashFsImpl.createHashClient(eventloop, serverInfos);
	}

	private static void uploadFile(final Eventloop eventloop, final ExecutorService executor, final HashFsImpl client, final String filename, Path sourcePath) {
		StreamConsumer<ByteBuf> consumer = client.upload(filename);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, sourcePath);
		producer.streamTo(consumer);
	}

	private static List<ServerInfo> prepareServerInfos(int serverCount) {
		List<ServerInfo> servers = new ArrayList<>();

		final int FIRST_SERVER_PORT = 5628;

		for (int i = 0; i < serverCount; i++) {
			InetSocketAddress address = new InetSocketAddress("127.0.0.1", FIRST_SERVER_PORT + i);
			servers.add(new ServerInfo(address, i, 1));
		}

		return servers;
	}

	private static HashFsGsonServer createServer(NioEventloop eventloop, Path fileStorage, List<ServerInfo> servers, ExecutorService executor,
	                                             ServerInfo serverInfo, Configuration configuration, ServerStatusNotifier serverStatusNotifier) throws IOException {
		HashFsClientProtocol protocol = new HashFsGsonClientProtocol(eventloop, servers);
		List<ServerStatusNotifier> notifiers = new ArrayList<>();
		notifiers.add(serverStatusNotifier);
		HashFsServer server = new HashFsServer(eventloop, serverInfo, servers, fileStorage, protocol, notifiers, executor, configuration);
		return HashFsGsonServer.createServerTransport(eventloop, serverInfo, server);
	}

	@Before
	public void before() throws Exception {
		dirPath = Paths.get(temporaryFolder.newFolder("test_data").toURI());

		Files.createDirectories(dirPath);

		Path tDir = dirPath.resolve("t_dir");
		Files.createDirectories(tDir);

		Path t1 = tDir.resolve("t1");
		Files.write(t1, ("some text\n\nmore text\t\n\n\r").getBytes(Charsets.UTF_8));

		Path t2 = tDir.resolve("t2");
		Files.write(t2, ("\n\raaa\nbbb").getBytes(Charsets.UTF_8));

		Path t3 = dirPath.resolve("empty_file");
		Files.createFile(t3);

		Path t4 = dirPath.resolve("a b");
		Files.write(t4, ("a\nb\nc").getBytes(Charsets.UTF_8));

		Path t5 = dirPath.resolve("big_file");

		StringBuilder testFileContentBuilder = new StringBuilder();
		for (int i = 0; i < 1000000; i++) {
			testFileContentBuilder.append(i);
			testFileContentBuilder.append("\n");
		}
		Files.write(t5, testFileContentBuilder.toString().getBytes(Charsets.UTF_8));
	}

	private void prepareServerWithFiles(final NioEventloop eventloop, List<ServerInfo> serverInfos,
	                                    CurrentServerFileMap currentServerFileMap, AtomicBoolean finishState) {
		final ExecutorService executor = Executors.newCachedThreadPool();

		Path tDir = dirPath.resolve("t_dir");

		HashFsImpl client = createClient(eventloop, serverInfos);

		uploadFile(eventloop, executor, client, "t_dir/t1", tDir.resolve("t1"));
		uploadFile(eventloop, executor, client, "t_dir/t2", tDir.resolve("t2"));
		uploadFile(eventloop, executor, client, "a b", dirPath.resolve("a b"));
		uploadFile(eventloop, executor, client, "big_file", dirPath.resolve("big_file"));
		uploadFile(eventloop, executor, client, "empty_file", dirPath.resolve("empty_file"));

		HashMultimap<Integer, String> expectedState = getInitServerWithFilesState();
		currentServerFileMap.setExpectedStateWithBreak(expectedState, eventloop, finishState);
	}

	private List<HashFsGsonServer> prepareServers(NioEventloop eventloop, List<ServerInfo> servers, ExecutorService executor,
	                                              ServerStatusNotifier serverStatusNotifier) throws IOException {
		List<HashFsGsonServer> totalServers = new ArrayList<>();
		// init servers
		Configuration configuration = new Configuration();
		for (int i = 0; i < servers.size(); i++) {
			ServerInfo serverInfo = servers.get(i);
			Path fileStorage = dirPath.resolve("server_storage_" + serverInfo.serverId);
			HashFsGsonServer server = createServer(eventloop, fileStorage, servers, executor, serverInfo, configuration, serverStatusNotifier);

			totalServers.add(server);
		}
		return totalServers;
	}

	private void closeServers(NioEventloop eventloop, List<HashFsGsonServer> servers, AtomicBoolean finishStatus, boolean closeValue) {
		for (HashFsGsonServer server : servers) {
			server.closeAll();
		}
		finishStatus.set(closeValue);
		eventloop.breakEventloop();
	}

	@Test
	public void testDeleteByUserWhileServerDown() throws Exception {

		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		prepareServerWithFiles(eventloop, serverInfos, currentServerFileMap, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		final int killIndex = 0;
		// kill server
		servers.get(killIndex).closeAll();

		// expected state after replicated
		final HashMultimap<Integer, String> expectedState = getInitServerWithFilesState();
		expectedState.put(1, "a b");
		expectedState.put(1, "empty_file");
		expectedState.put(2, "t_dir/t1");
		expectedState.put(2, "big_file");

		currentServerFileMap.setExpectedStateWithBreak(expectedState, eventloop, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		// delete file
		final HashFs client = createClient(eventloop, serverInfos);
		client.deleteFile("t_dir/t1", new ResultCallback<Boolean>() {
			@Override
			public void onResult(Boolean result) {
				finishSucceed.set(result);
			}

			@Override
			public void onException(Exception exception) {
				finishSucceed.set(false);
			}
		});

		expectedState.remove(1, "t_dir/t1");
		expectedState.remove(2, "t_dir/t1");
		expectedState.remove(4, "t_dir/t1");

		currentServerFileMap.setExpectedStateWithBreak(expectedState, eventloop, finishSucceed);
		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		// up server

		Configuration configuration = new Configuration();
		ServerInfo serverInfo = serverInfos.get(killIndex);
		Path fileStorage = dirPath.resolve("server_storage_" + serverInfo.serverId);
		HashFsGsonServer server = createServer(eventloop, fileStorage, serverInfos, executor, serverInfo, configuration, serverStatusNotifier);
		servers.set(killIndex, server);

		expectedState.remove(0, "t_dir/t1");
		expectedState.remove(1, "a b");
		expectedState.remove(1, "empty_file");
		expectedState.remove(2, "big_file");

		currentServerFileMap.setExpectedStateWithClose(expectedState, eventloop, servers, finishSucceed);
		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

	@Test
	public void testDeleteByUser() throws Exception {
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		prepareServerWithFiles(eventloop, serverInfos, currentServerFileMap, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		final HashFs client = createClient(eventloop, serverInfos);
		client.deleteFile("t_dir/t1", new ResultCallback<Boolean>() {
			@Override
			public void onResult(Boolean result) {
				finishSucceed.set(result);
			}

			@Override
			public void onException(Exception exception) {
				finishSucceed.set(false);
			}
		});

		final HashMultimap<Integer, String> expectedState = getInitServerWithFilesState();

		expectedState.remove(0, "t_dir/t1");
		expectedState.remove(1, "t_dir/t1");
		expectedState.remove(4, "t_dir/t1");

		currentServerFileMap.setExpectedStateWithClose(expectedState, eventloop, servers, finishSucceed);
		eventloop.run();

		Assert.assertTrue(finishSucceed.get());
	}


	@Test
	public void downloadBigFile() throws Exception {
		download("big_file", "copy_big_file");
	}

	@Test
	public void downloadInnerFile() throws Exception {
		download("t_dir/t1", "copy_t1");
	}


	public void download(final String fileName, String copyFile) throws Exception {
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		prepareServerWithFiles(eventloop, serverInfos, currentServerFileMap, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		HashFsImpl client = HashFsImpl.createHashClient(eventloop, serverInfos);
		StreamProducer<ByteBuf> producer = client.download(fileName);
		final Path destination = dirPath.resolve(copyFile);
		StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor, destination, true);
		producer.streamTo(diskWrite);
		diskWrite.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				boolean fileContentEquals = false;
				try {
					fileContentEquals = com.google.common.io.Files.equal(dirPath.resolve(fileName).toFile(), destination.toFile());
				} catch (IOException e) {
					logger.error("Can't compare files", e);
				}
				logger.info("Download file equals to uploaded: {}.", fileContentEquals);
				closeServers(eventloop, servers, finishSucceed, fileContentEquals);
			}

			@Override
			public void onException(Exception exception) {
				closeServers(eventloop, servers, finishSucceed, false);
			}
		});

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

	@Test
	public void testDownUp() throws Exception {

		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		prepareServerWithFiles(eventloop, serverInfos, currentServerFileMap, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		final int killIndex = 2;
		// kill server
		servers.get(killIndex).closeAll();

		final HashMultimap<Integer, String> expectedState = getInitServerWithFilesState();

		expectedState.put(0, "t_dir/t2");
		expectedState.put(1, "a b");
		expectedState.put(1, "empty_file");

		currentServerFileMap.setExpectedStateWithBreak(expectedState, eventloop, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());

		// up server
		Configuration configuration = new Configuration();
		ServerInfo serverInfo = serverInfos.get(killIndex);
		Path fileStorage = dirPath.resolve("server_storage_" + serverInfo.serverId);
		HashFsGsonServer server = createServer(eventloop, fileStorage, serverInfos, executor, serverInfo, configuration, serverStatusNotifier);
		servers.set(killIndex, server);

		final HashMultimap<Integer, String> initState = getInitServerWithFilesState();
		currentServerFileMap.setExpectedStateWithClose(initState, eventloop, servers, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

	@Test
	public void testUploadLotFiles() throws IOException {
		final int filesCount = 50;
		List<String> createdFiles = new ArrayList<>();

		for (int i = 0; i < filesCount; i++) {
			String filename = "test_file_" + i;
			Path filePath = dirPath.resolve(filename);
			Files.write(filePath, ("some text\nmore text").getBytes(Charsets.UTF_8));
			createdFiles.add(filename);
		}

		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		HashMultimap<Integer, String> expectedState = HashMultimap.create();

		for (String filename : createdFiles) {
			HashFsImpl client = createClient(eventloop, serverInfos);
			List<ServerInfo> orderedServers = RendezvousHashing.sortServers(serverInfos, filename);

			StreamConsumer<ByteBuf> consumer = client.upload(filename);
			final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, dirPath.resolve(filename));
			producer.streamTo(consumer);

			for (ServerInfo serverInfo : orderedServers.subList(0, 3)) {
				expectedState.put(serverInfo.serverId, filename);
			}
		}

		currentServerFileMap.setExpectedStateWithClose(expectedState, eventloop, servers, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

	@Test
	public void testUploadFile() throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		final String fileName = "t_dir/t1";

		HashFsImpl client = createClient(eventloop, serverInfos);

		StreamConsumer<ByteBuf> consumer = client.upload(fileName);
		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, dirPath.resolve(fileName));
		producer.streamTo(consumer);

		HashMultimap<Integer, String> expectedState = HashMultimap.create();
		expectedState.put(0, fileName);
		expectedState.put(1, fileName);
		expectedState.put(4, fileName);

		currentServerFileMap.setExpectedStateWithClose(expectedState, eventloop, servers, finishSucceed);

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

	@Test
	public void testReUploadFile() throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		final String filename = "t_dir/t1";

		HashFsImpl client = createClient(eventloop, serverInfos);

		StreamConsumer<ByteBuf> consumer = client.upload(filename);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, dirPath.resolve(filename));
		producer.streamTo(consumer);

		HashMultimap<Integer, String> expectedState = HashMultimap.create();
		expectedState.put(0, filename);
		expectedState.put(1, filename);
		expectedState.put(4, filename);

		currentServerFileMap.setExpectedStateWithBreak(expectedState, eventloop, finishSucceed);
		eventloop.run();

		StreamConsumer<ByteBuf> consumerSecond = client.upload(filename);

		final StreamFileReader producerSecond = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, dirPath.resolve(filename));
		producerSecond.streamTo(consumerSecond);
		consumerSecond.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				closeServers(eventloop, servers, finishSucceed, false);
			}

			@Override
			public void onException(Exception exception) {
				closeServers(eventloop, servers, finishSucceed, true);
			}
		});

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

	@Test
	public void testDownloadFileNotExists() throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final int serverCount = 5;

		final AtomicBoolean finishSucceed = new AtomicBoolean(true);
		final CurrentServerFileMap currentServerFileMap = new CurrentServerFileMap();
		final SimpleServerStatusNotifier serverStatusNotifier = new SimpleServerStatusNotifier(currentServerFileMap);

		final List<ServerInfo> serverInfos = prepareServerInfos(serverCount);
		final List<HashFsGsonServer> servers = prepareServers(eventloop, serverInfos, executor, serverStatusNotifier);

		HashFsImpl client = HashFsImpl.createHashClient(eventloop, serverInfos);
		StreamProducer<ByteBuf> producer = client.download("t_dir/t1");
		final Path destination = dirPath.resolve("copy_t1");
		StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor, destination, true);
		producer.streamTo(diskWrite);
		diskWrite.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				closeServers(eventloop, servers, finishSucceed, false);
			}

			@Override
			public void onException(Exception exception) {
				logger.info("Write to disk exception: {}", exception.getMessage());
				closeServers(eventloop, servers, finishSucceed, true);
			}
		});

		eventloop.run();
		Assert.assertTrue(finishSucceed.get());
	}

}
