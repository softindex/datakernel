/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.trigger;

import io.datakernel.common.api.Initializable;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.jmx.api.ConcurrentJmxBean;
import io.datakernel.jmx.api.attribute.JmxAttribute;
import io.datakernel.jmx.api.attribute.JmxOperation;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.datakernel.common.collection.CollectionUtils.difference;
import static io.datakernel.common.jmx.MBeanFormat.formatListAsMultilineString;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class Triggers implements ConcurrentJmxBean, Initializable<Triggers> {
	public static final Duration CACHE_TIMEOUT = Duration.ofSeconds(1);

	private final List<Trigger> triggers = new ArrayList<>();
	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private Triggers() {
	}

	public static Triggers create() {
		return new Triggers();
	}

	private static final class TriggerKey {
		private final String component;
		private final String name;

		private TriggerKey(String component, String name) {
			this.component = component;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TriggerKey that = (TriggerKey) o;
			return Objects.equals(component, that.component) &&
					Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(component, name);
		}
	}

	private final Map<Trigger, TriggerResult> suppressedResults = new LinkedHashMap<>();
	private final Map<Trigger, TriggerResult> cachedResults = new LinkedHashMap<>();
	private Map<Trigger, TriggerWithResult> maxSeverityResults = new LinkedHashMap<>();
	private long cachedTimestamp;

	private final Predicate<TriggerWithResult> isNotSuppressed = triggerWithResult -> {
		Trigger trigger = triggerWithResult.trigger;
		if (suppressedResults.containsKey(trigger)) {
			TriggerResult suppressedTriggerResult = suppressedResults.get(trigger);
			TriggerResult triggerResult = triggerWithResult.getTriggerResult();

			return triggerResult.getCount() > suppressedTriggerResult.getCount() ||
					triggerResult.getTimestamp() > suppressedTriggerResult.getTimestamp();
		}
		return true;
	};

	public Triggers withTrigger(Trigger trigger) {
		this.triggers.add(trigger);
		return this;
	}

	public Triggers withTrigger(Severity severity, String component, String name, Supplier<TriggerResult> triggerFunction) {
		return withTrigger(Trigger.of(severity, component, name, triggerFunction));
	}

	synchronized public void addTrigger(Trigger trigger) {
		withTrigger(trigger);
	}

	synchronized public void addTrigger(Severity severity, String component, String name, Supplier<TriggerResult> triggerFunction) {
		withTrigger(severity, component, name, triggerFunction);
	}

	private void refresh() {
		long currentTime = now.currentTimeMillis();
		if (cachedTimestamp + CACHE_TIMEOUT.toMillis() < currentTime) {
			cachedTimestamp = currentTime;

			Map<Trigger, TriggerResult> newResults = new HashMap<>();
			for (Trigger trigger : triggers) {
				TriggerResult newResult;
				try {
					newResult = trigger.getTriggerFunction().get();
				} catch (Exception e) {
					newResult = TriggerResult.ofError(e);
				}
				if (newResult != null && newResult.isPresent()) {
					newResults.put(trigger, newResult);
				}
			}
			for (Trigger trigger : difference(cachedResults.keySet(), newResults.keySet())) {
				cachedResults.remove(trigger);
				suppressedResults.remove(trigger);
			}
			for (Trigger trigger : newResults.keySet()) {
				TriggerResult newResult = newResults.get(trigger);
				if (!newResult.hasTimestamp()) {
					TriggerResult oldResult = cachedResults.get(trigger);
					newResult = TriggerResult.create(
							oldResult == null ? currentTime : oldResult.getTimestamp(),
							newResult.getThrowable(),
							newResult.getValue());
				}
				cachedResults.put(trigger, newResult.withCount(0));
			}
			for (Trigger trigger : newResults.keySet()) {
				TriggerResult oldResult = cachedResults.get(trigger);
				TriggerResult newResult = newResults.get(trigger);
				cachedResults.put(trigger, oldResult.withCount(oldResult.getCount() + newResult.getCount()));
			}
			maxSeverityResults = new HashMap<>(cachedResults.size());
			for (Map.Entry<Trigger, TriggerResult> entry : cachedResults.entrySet()) {
				Trigger trigger = entry.getKey();
				TriggerResult triggerResult = entry.getValue();

				TriggerWithResult oldTriggerWithResult = maxSeverityResults.get(trigger);
				if (oldTriggerWithResult == null ||
						oldTriggerWithResult.getTrigger().getSeverity().ordinal() < trigger.getSeverity().ordinal() ||
						oldTriggerWithResult.getTrigger().getSeverity() == trigger.getSeverity() &&
								oldTriggerWithResult.getTriggerResult().getTimestamp() > triggerResult.getTimestamp()) {
					maxSeverityResults.put(trigger, new TriggerWithResult(trigger, triggerResult
							.withCount(triggerResult.getCount())));
				} else {
					maxSeverityResults.put(trigger, new TriggerWithResult(oldTriggerWithResult.getTrigger(), oldTriggerWithResult.getTriggerResult()
							.withCount(triggerResult.getCount())));
				}
			}

		}
	}

	public static final class TriggerWithResult {
		private final Trigger trigger;
		private final TriggerResult triggerResult;

		public TriggerWithResult(Trigger trigger, TriggerResult triggerResult) {
			this.trigger = trigger;
			this.triggerResult = triggerResult;
		}

		public Trigger getTrigger() {
			return trigger;
		}

		public TriggerResult getTriggerResult() {
			return triggerResult;
		}

		@Override
		public String toString() {
			return trigger + " :: " + triggerResult;
		}
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResultsDebug() {
		return getResultsBySeverity(Severity.DEBUG);
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResultsInformation() {
		return getResultsBySeverity(Severity.INFORMATION);
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResultsWarning() {
		return getResultsBySeverity(Severity.WARNING);
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResultsAverage() {
		return getResultsBySeverity(Severity.AVERAGE);
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResultsHigh() {
		return getResultsBySeverity(Severity.HIGH);
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResultsDisaster() {
		return getResultsBySeverity(Severity.DISASTER);
	}

	private List<TriggerWithResult> getResultsBySeverity(@Nullable Severity severity) {
		refresh();
		return maxSeverityResults.values().stream()
				.filter(isNotSuppressed)
				.filter(entry -> entry.getTrigger().getSeverity() == severity)
				.sorted(comparing(item -> item.getTriggerResult().getTimestamp()))
				.collect(Collectors.groupingBy(o -> new TriggerKey(o.getTrigger().getComponent(), o.getTrigger().getName())))
				.values()
				.stream()
				.flatMap(list -> list.stream()
						.filter(trigger -> trigger.getTrigger().getSeverity() == list.get(list.size() - 1).getTrigger().getSeverity()))
				.collect(Collectors.toList());
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResults() {
		refresh();
		return maxSeverityResults.values().stream()
				.filter(isNotSuppressed)
				.sorted(Comparator.<TriggerWithResult, Severity>comparing(item -> item.getTrigger().getSeverity())
						.thenComparing(item -> item.getTriggerResult().getTimestamp()))
				.collect(Collectors.groupingBy(o -> new TriggerKey(o.getTrigger().getComponent(), o.getTrigger().getName())))
				.values()
				.stream()
				.flatMap(list -> list.stream()
						.filter(trigger -> trigger.getTrigger().getSeverity() == list.get(list.size() - 1).getTrigger().getSeverity()))
				.collect(Collectors.toList());
	}

	@JmxAttribute
	synchronized public String getMultilineSuppressedResults() {
		return formatListAsMultilineString(new ArrayList<>(suppressedResults.keySet()));
	}

	@JmxAttribute
	public String getMultilineResultsDebug() {
		return formatListAsMultilineString(getResultsBySeverity(Severity.DEBUG));
	}

	@JmxAttribute
	public String getMultilineResultsInformation() {
		return formatListAsMultilineString(getResultsBySeverity(Severity.INFORMATION));
	}

	@JmxAttribute
	public String getMultilineResultsWarning() {
		return formatListAsMultilineString(getResultsBySeverity(Severity.WARNING));
	}

	@JmxAttribute
	public String getMultilineResultsAverage() {
		return formatListAsMultilineString(getResultsBySeverity(Severity.AVERAGE));
	}

	@JmxAttribute
	public String getMultilineResultsHigh() {
		return formatListAsMultilineString(getResultsBySeverity(Severity.HIGH));
	}

	@JmxAttribute
	public String getMultilineResultsDisaster() {
		return formatListAsMultilineString(getResultsBySeverity(Severity.DISASTER));
	}

	@JmxAttribute
	public String getMultilineResults() {
		return formatListAsMultilineString(getResults());
	}

	@JmxAttribute
	@Nullable
	public synchronized Severity getMaxSeverity() {
		refresh();
		return maxSeverityResults.values().stream()
				.filter(isNotSuppressed)
				.max(comparing(entry -> entry.getTrigger().getSeverity()))
				.map(entry -> entry.getTrigger().getSeverity())
				.orElse(null);
	}

	@JmxAttribute
	@Nullable
	public synchronized String getMaxSeverityResult() {
		refresh();
		return maxSeverityResults.values().stream()
				.filter(isNotSuppressed)
				.max(comparing(entry -> entry.getTrigger().getSeverity()))
				.map(Object::toString)
				.orElse(null);
	}

	@JmxAttribute
	synchronized public List<String> getTriggers() {
		return triggers.stream()
				.sorted(comparing(Trigger::getSeverity).reversed().thenComparing(Trigger::getComponent).thenComparing(Trigger::getName))
				.map(t -> t.getSeverity() + " : " + t.getComponent() + " : " + t.getName())
				.distinct()
				.collect(toList());
	}

	@JmxAttribute
	synchronized public List<String> getTriggerNames() {
		return triggers.stream()
				.sorted(comparing(Trigger::getComponent).thenComparing(Trigger::getName))
				.map(t -> t.getComponent() + " : " + t.getName())
				.distinct()
				.collect(toList());
	}

	@JmxAttribute
	synchronized public String getTriggerComponents() {
		return triggers.stream()
				.sorted(comparing(Trigger::getComponent))
				.map(Trigger::getComponent)
				.distinct()
				.collect(joining(", "));
	}

	@JmxOperation
	public synchronized void suppressAllTriggers() {
		suppressBy(trigger -> true);
	}

	@JmxOperation
	public synchronized void suppressTriggerByName(String name) {
		suppressBy(trigger -> trigger.getName().equals(name));
	}

	@JmxOperation
	public synchronized void suppressTriggerByComponent(String component) {
		suppressBy(trigger -> trigger.getComponent().equals(component));
	}

	@JmxOperation
	public synchronized void suppressTriggerBySeverity(String severity) {
		suppressBy(trigger -> trigger.getSeverity().name().equalsIgnoreCase(severity));
	}

	/**
	 * @param signature Trigger signature in a form of <i>"Severity:Component:Name"</i>
	 */
	@JmxOperation
	public synchronized void suppressTriggersBySignature(String signature) {
		String[] values = signature.split(":");
		if (values.length != 3) {
			return;
		}

		suppressBy(trigger ->
				trigger.getSeverity().name().equalsIgnoreCase(values[0].trim()) &&
						trigger.getComponent().equals(values[1].trim()) &&
						trigger.getName().equals(values[2].trim()));
	}

	private void suppressBy(Predicate<Trigger> condition) {
		refresh();
		cachedResults.keySet().stream()
				.filter(condition)
				.forEach(trigger -> suppressedResults.put(trigger, cachedResults.get(trigger)));
	}

	@Override
	public String toString() {
		return getTriggerComponents();
	}
}
