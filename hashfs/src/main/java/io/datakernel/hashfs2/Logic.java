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

package io.datakernel.hashfs2;

import io.datakernel.async.ResultCallback;

import java.util.Set;

public interface Logic {
	void update();

	boolean canUpload(String filePath);

	void onUploadStart(String filePath);

	void onUploadComplete(String filePath);

	void onUploadFailed(String filePath);

	boolean canApprove(String filePath);

	void onApprove(String filePath);

	void onApproveCancel(String filePath);

	boolean canDownload(String filePath);

	void onDownloadStart(String filePath);

	void onDownloadComplete(String filePath);

	void onDownloadFailed(String filePath);

	boolean canDelete(String filePath);

	void onDeletionStart(String filePath);

	void onDeleteComplete(String filePath);

	void onDeleteFailed(String filePath);

	void onShowAliveRequest(ResultCallback<Set<ServerInfo>> alive);

	void onShowAliveResponse(Set<ServerInfo> result, long timestamp);

	void onOfferRequest(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result);

	void onReplicationComplete(String filePath, ServerInfo server);

	void onReplicationFailed(String filePath, ServerInfo server);
}
