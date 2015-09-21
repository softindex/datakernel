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

package io.datakernel.examples;

import com.google.common.base.MoreObjects;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Output record consists of a word and list of document ids in which this word appears.
 */
public class InvertedIndexQueryResult {
	public String word;
	public List<Integer> documents;

	public InvertedIndexQueryResult() {
	}

	public InvertedIndexQueryResult(String word, List<Integer> documents) {
		this.word = word;
		this.documents = documents;
	}

	public static final List<String> OUTPUT_FIELDS = asList("documents");

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		InvertedIndexQueryResult that = (InvertedIndexQueryResult) o;

		if (word != null ? !word.equals(that.word) : that.word != null) return false;
		return !(documents != null ? !documents.equals(that.documents) : that.documents != null);
	}

	@Override
	public int hashCode() {
		int result = word != null ? word.hashCode() : 0;
		result = 31 * result + (documents != null ? documents.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("word", word)
				.add("documents", documents)
				.toString();
	}
}
