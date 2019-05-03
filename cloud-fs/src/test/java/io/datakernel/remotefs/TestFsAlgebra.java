/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.remotefs;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import static io.datakernel.async.TestUtils.await;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public final class TestFsAlgebra {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private FsClient local;

	@Before
	public void setup() throws IOException {
		local = LocalFsClient.create(Eventloop.getCurrentEventloop(), temporaryFolder.newFolder("test").toPath());
	}

	private void upload(FsClient client, String filename) {
		await(client.upload(filename).then(consumer -> consumer.accept(null)));
	}

	private void expect(String... realFiles) {
		assertEquals(Arrays.stream(realFiles).collect(toSet()), await(local.list("**")).stream().map(FileMetadata::getName).collect(toSet()));
	}

	@Test
	public void addingPrefix() {
		FsClient prefixed = local.addingPrefix("prefix/");

		upload(prefixed, "test.txt");
		upload(prefixed, "deeper/test.txt");

		expect("prefix/test.txt", "prefix/deeper/test.txt");
	}

	@Test
	public void strippingPrefix() {
		FsClient prefixed = local.strippingPrefix("prefix/");

		upload(prefixed, "prefix/test.txt");
		upload(prefixed, "prefix/deeper/test.txt");
		upload(prefixed, "nonPrefix/test.txt");

		expect("test.txt", "deeper/test.txt");
	}

	@Test
	public void mountingClient() {
		FsClient root = local.subfolder("root");
		FsClient first = local.subfolder("first");
		FsClient second = local.subfolder("second");
		FsClient third = local.subfolder("third");

		FsClient mounted = root
				.mount("hello", first)
				.mount("test/inner", second)
				.mount("last", third);

		//   /           ->  /root
		//   /hello      ->  /first
		//   /test/inner ->  /second
		//   /last       ->  /third

		upload(mounted, "test1.txt");
		upload(mounted, "hello/test2.txt");
		upload(mounted, "test/test3.txt");
		upload(mounted, "test/inner/test4.txt");
		upload(mounted, "last/test5.txt");

		expect("root/test1.txt", "first/test2.txt", "root/test/test3.txt", "second/test4.txt", "third/test5.txt");
	}

	@Test
	public void filterClient() {
		FsClient filtered = local.filter(s -> s.endsWith(".txt") && Pattern.compile("\\d{2}").matcher(s).find());

		upload(filtered, "test2.txt");
		upload(filtered, "test22.txt");
		upload(filtered, "test22.jpg");
		upload(filtered, "123.txt");

		expect("test22.txt", "123.txt");
	}
}
