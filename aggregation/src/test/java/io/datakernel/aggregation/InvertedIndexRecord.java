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

package io.datakernel.aggregation;

import io.datakernel.aggregation.annotation.Key;
import io.datakernel.aggregation.annotation.Measure;

/**
 * First we define the structure of the input record which represents the word and id of the document that contains this word.
 */
public class InvertedIndexRecord {
	private String word;
	private int documentId;

	@Key("word")
	public String getWord() {
		return word;
	}

	@Measure("documents")
	public Integer getDocumentId() {
		return documentId;
	}

	public InvertedIndexRecord(String word, int documentId) {
		this.word = word;
		this.documentId = documentId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		InvertedIndexRecord that = (InvertedIndexRecord) o;

		if (word != null ? !word.equals(that.word) : that.word != null) return false;
		return documentId == that.documentId;

	}

	@Override
	public int hashCode() {
		int result = word != null ? word.hashCode() : 0;
		result = 31 * result + documentId;
		return result;
	}

	@Override
	public String toString() {
		return "InvertedIndexRecord{" +
				"word='" + word + '\'' +
				", documentId=" + documentId +
				'}';
	}
}

