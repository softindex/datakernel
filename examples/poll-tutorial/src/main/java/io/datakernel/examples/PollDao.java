package io.datakernel.examples;

import java.util.*;

//[START EXAMPLE]
public interface PollDao {
	Poll find(int id);
	Map<Integer, Poll> findAll();
	int add(Poll poll);
	void update(int id, Poll poll);
	void remove(int id);

	class Poll {
		private String title;
		private String message;
		private Map<String, Integer> optionToVotes;

		public Poll(String title, String message, List<String> options) {
			this.title = title;
			this.message = message;
			this.optionToVotes = new LinkedHashMap<>();

			for (String option : options) {
				optionToVotes.put(option, 0);
			}
		}

		public void vote(String option) {
			Integer votesCount = optionToVotes.get(option);
			optionToVotes.put(option, ++votesCount);
		}

		public String getTitle() {
			return title;
		}

		public String getMessage() {
			return message;
		}

		public Set<Map.Entry<String, Integer>> getOptions() {
			return optionToVotes.entrySet();
		}

	}
}
//[END EXAMPLE]
