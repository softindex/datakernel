package io.global.comm.http.view;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class PageView {
	private static final int SHOWN_PAGES = 5;

	private final List<Void> tail;

	private final int next;
	private final int prev;
	private final int last;

	private List<PageNumber> numbers;
	private final int current;

	private final boolean firstSelected;
	private final boolean lastSelected;

	public PageView(int page, int limit, int all) {
		this.current = page;

		int last = (all + limit - 1) / limit;
		this.last = last - 1;

		this.firstSelected = page <= 0;
		this.lastSelected = page >= last - 1;

		this.next = page < last - 1 ? page + 1 : page;
		this.prev = page < 1 ? 0 : page - 1;

		this.tail = lastSelected ? IntStream.range(0, last * limit - all).mapToObj($ -> (Void) null).collect(toList()) : emptyList();

		int halfShown = SHOWN_PAGES / 2;

		int from, to;
		if (last <= SHOWN_PAGES) {
			from = 0;
			to = last;
		} else if (current < halfShown) {
			from = 0;
			to = SHOWN_PAGES;
		} else if (current > last - halfShown - 1) {
			from = last - SHOWN_PAGES;
			to = last;
		} else {
			from = current - halfShown;
			to = current + halfShown + 1;
		}

		this.numbers = IntStream.range(from, to).mapToObj(i -> new PageNumber(i, i == current)).collect(toList());
	}

	public int getNext() {
		return next;
	}

	public int getPrev() {
		return prev;
	}

	public int getLast() {
		return last;
	}

	public List<PageNumber> getNumbers() {
		return numbers;
	}

	public boolean isFirstSelected() {
		return firstSelected;
	}

	public boolean isLastSelected() {
		return lastSelected;
	}

	public List<Void> getTail() {
		return tail;
	}

	public static final class PageNumber {
		private final int page;
		private final boolean current;

		private PageNumber(int page, boolean current) {
			this.page = page;
			this.current = current;
		}

		public int getShown() {
			return page + 1;
		}

		public int getReal() {
			return page;
		}

		public boolean isCurrent() {
			return current;
		}
	}
}
