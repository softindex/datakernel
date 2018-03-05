package io.datakernel.trigger;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.util.Initializer;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.datakernel.util.CollectionUtils.difference;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class Triggers implements ConcurrentJmxMBean, Initializer<Triggers> {
	public static final long CACHE_TIMEOUT = 1000L;

	private final List<Trigger> triggers = new ArrayList<>();

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

	private Map<Trigger, TriggerResult> cachedResults = new LinkedHashMap<>();
	private Map<TriggerKey, TriggerWithResult> maxSeverityResults = new LinkedHashMap<>();
	private long cachedTimestamp;

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
		long now = System.currentTimeMillis();
		if (cachedTimestamp + CACHE_TIMEOUT < now) {
			cachedTimestamp = now;
			Map<Trigger, TriggerResult> newResults = new HashMap<>();
			for (Trigger trigger : triggers) {
				TriggerResult newResult = trigger.getTriggerFunction().get();
				if (newResult != null && newResult.isPresent()) {
					newResults.put(trigger, newResult);
				}
			}
			for (Trigger trigger : difference(cachedResults.keySet(), newResults.keySet())) {
				cachedResults.remove(trigger);
			}
			for (Trigger trigger : newResults.keySet()) {
				TriggerResult oldResult = cachedResults.get(trigger);
				TriggerResult newResult = newResults.get(trigger);
				if (!newResult.hasTimestamp() || oldResult != null) {
					newResult = TriggerResult.create(
							oldResult == null ? now : oldResult.getTimestamp(),
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

				TriggerKey triggerKey = new TriggerKey(trigger.getComponent(), trigger.getName());
				TriggerWithResult oldTriggerWithResult = maxSeverityResults.get(triggerKey);
				if (oldTriggerWithResult == null ||
						oldTriggerWithResult.getTrigger().getSeverity().ordinal() < trigger.getSeverity().ordinal() ||
						(oldTriggerWithResult.getTrigger().getSeverity() == trigger.getSeverity() &&
								oldTriggerWithResult.getTriggerResult().getTimestamp() > triggerResult.getTimestamp())) {
					maxSeverityResults.put(triggerKey, new TriggerWithResult(trigger, triggerResult
							.withCount(triggerResult.getCount() +
									(oldTriggerWithResult != null ? oldTriggerWithResult.getTriggerResult().getCount() : 0))));
				} else {
					maxSeverityResults.put(triggerKey, new TriggerWithResult(trigger, oldTriggerWithResult.getTriggerResult()
							.withCount(triggerResult.getCount() +
									oldTriggerWithResult.getTriggerResult().getCount())));
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
				.filter(entry -> entry.getTrigger().getSeverity() == severity)
				.sorted(comparing(item -> item.getTriggerResult().getTimestamp()))
				.collect(Collectors.toList());
	}

	@JmxAttribute
	synchronized public List<TriggerWithResult> getResults() {
		refresh();
		return maxSeverityResults.values().stream()
				.sorted(Comparator.<TriggerWithResult, Severity>comparing(item -> item.getTrigger().getSeverity())
						.thenComparing(item -> item.getTriggerResult().getTimestamp()))
				.collect(Collectors.toList());
	}

	@JmxAttribute
	synchronized public Severity getMaxSeverity() {
		refresh();
		return maxSeverityResults.values().stream()
				.max(comparing(entry -> entry.getTrigger().getSeverity()))
				.map(entry -> entry.getTrigger().getSeverity())
				.orElse(null);
	}

	@JmxAttribute
	synchronized public String getMaxSeverityResult() {
		refresh();
		return maxSeverityResults.values().stream()
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

	@Override
	public String toString() {
		return getTriggerComponents();
	}
}
