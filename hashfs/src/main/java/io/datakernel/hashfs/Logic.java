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

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioService;

import java.util.Set;

public interface Logic extends NioService {
	void update(long timestamp);

	boolean canUpload(String fileName);

	void onUploadStart(String fileName);

	void onUploadComplete(String fileName);

	void onUploadFailed(String fileName);

	boolean canApprove(String fileName);

	void onApprove(String fileName);

	void onApproveCancel(String fileName);

	boolean canDownload(String fileName);

	void onDownloadStart(String fileName);

	void onDownloadComplete(String fileName);

	void onDownloadFailed(String fileName);

	boolean canDelete(String fileName);

	void onDeletionStart(String fileName);

	void onDeleteComplete(String fileName);

	void onDeleteFailed(String fileName);

	void onShowAliveRequest(long timestamp, ResultCallback<Set<ServerInfo>> callback);

	void onShowAliveResponse(long timestamp, Set<ServerInfo> result);

	void onOfferRequest(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback);

	void onReplicationStart(String fileName);

	void onReplicationComplete(ServerInfo server, String fileName);

	void onReplicationFailed(ServerInfo server, String filePath);
}