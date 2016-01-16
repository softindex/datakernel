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

package io.datakernel.eventloop;

import java.io.IOException;

/**
 * Represents non-blocking server which listens new connection and accepts it asynchronous.
 * It is {@link AcceptCallback} for handling accepting to this server.
 */
public interface EventloopServer extends AcceptCallback {
	Eventloop getEventloop();

	/**
	 * Tells the NioServer to start listen on its port and hostname.
	 *
	 * @throws IOException if the socket can not be created.
	 */
	void listen() throws IOException;

	/**
	 * Closes the server. Any open channels will be closed.
	 */
	void close();
}
