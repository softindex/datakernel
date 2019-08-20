package io.datakernel.docs.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

@SuppressWarnings("unused")
public class NavBar {
	private final Map<String, List<Item>> destinations;

	private NavBar(Map<String, List<Item>> destinations) {
		this.destinations = destinations;
	}

	public Map<String, List<Item>> getDestinations() {
		return destinations;
	}

	public Set<Map.Entry<String, List<Item>>> getDestinationsSet() {
		return destinations.entrySet();
	}

	public static NavBar of(Map<String, List<Item>> destinations) {
		return new NavBar(destinations);
	}

	public static NavBar empty() {
		return new NavBar(emptyMap());
	}

	public static final class Item {
		private final String id;
		private final String filename;
		private final String docTitle;
		private final boolean selected;

		private Item(String id, String filename, String docTitle, boolean selected) {
			this.id = id;
			this.filename = filename;
			this.docTitle = docTitle;
			this.selected = selected;
		}

		public static Item of(String id, String filename, String docTitle, boolean selected) {
			return new Item(id, filename, docTitle, selected);
		}

		public boolean isSelected() {
			return selected;
		}

		public String getId() {
			return id;
		}

		public String getFilename() {
			return filename;
		}

		public String getDocTitle() {
			return docTitle;
		}
	}
}
