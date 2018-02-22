package io.datakernel.trigger;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.util.Initializer;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.datakernel.jmx.MBeanFormat.formatTimestamp;
import static io.datakernel.util.CollectionUtils.difference;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class Triggers implements ConcurrentJmxMBean, Initializer<Triggers> {
	public static final long CACHE_TIMEOUT = 1000L;

	private final List<Trigger> triggers = new ArrayList<>();
	private Consumer<Triggers> initializer;

	private Triggers() {
	}

	public static Triggers create() {
		return new Triggers();
	}

	public Triggers withInitializer(Consumer<Triggers> initializer) {
		this.initializer = initializer;
		return this;
	}

	private static final class CachedTriggerResult {
		private final String description;
		private final long timestamp;

		private CachedTriggerResult(String description, long timestamp) {
			this.description = description;
			this.timestamp = timestamp;
		}
	}

	private Map<Trigger, CachedTriggerResult> cachedResults = new LinkedHashMap<>();
	private long cachedTimestamp;

	public Triggers withTrigger(Trigger trigger) {
		this.triggers.add(trigger);
		return this;
	}

	public Triggers withTrigger(Severity severity, String name, Supplier<String> triggerFunction) {
		return withTrigger(new Trigger(severity, name, triggerFunction));
	}

	public <T> Triggers withTrigger(Severity severity, String name, T instance, Function<T, String> triggerFunction) {
		return withTrigger(severity, name, () -> triggerFunction.apply(instance));
	}

	public <T> Triggers withTrigger(Severity severity, String name, T instance, Predicate<T> triggerPredicate, String description) {
		return withTrigger(severity, name, () -> triggerPredicate.test(instance) ? description : null);
	}

	synchronized public void addTrigger(Trigger trigger) {
		withTrigger(trigger);
	}

	synchronized public void addTrigger(Severity severity, String name, Supplier<String> triggerFunction) {
		withTrigger(severity, name, triggerFunction);
	}

	synchronized public <T> void addTrigger(Severity severity, String name, T instance, Function<T, String> triggerFunction) {
		withTrigger(severity, name, instance, triggerFunction);
	}

	synchronized public <T> void addTrigger(Severity severity, String name, T instance, Predicate<T> triggerPredicate, String description) {
		withTrigger(severity, name, instance, triggerPredicate, description);
	}

	private void refresh() {
		if (initializer != null) {
			Consumer<Triggers> initializer = this.initializer;
			this.initializer = null;
			initializer.accept(this);
		}
		long now = System.currentTimeMillis();
		if (cachedTimestamp + CACHE_TIMEOUT < now) {
			cachedTimestamp = now;
			Map<Trigger, String> results = new HashMap<>();
			for (Trigger trigger : triggers) {
				Supplier<String> triggerFunction = trigger.getTriggerFunction();
				String value = triggerFunction.get();
				if (value != null) {
					results.put(trigger, value);
				}
			}
			for (Trigger trigger : difference(cachedResults.keySet(), results.keySet())) {
				cachedResults.remove(trigger);
			}
			for (Trigger trigger : results.keySet()) {
				cachedResults.putIfAbsent(trigger, new CachedTriggerResult(results.get(trigger), now));
			}
		}
	}

	public static final class TriggerResult {
		private final Severity severity;
		private final String name;
		private final String description;
		private final long timestamp;

		public TriggerResult(Severity severity, String name, String description, long timestamp) {
			this.severity = severity;
			this.name = name;
			this.description = description;
			this.timestamp = timestamp;
		}

		public Severity getSeverity() {
			return severity;
		}

		public String getName() {
			return name;
		}

		public String getDescription() {
			return description;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return (severity != null ? severity + " : " : "") + name + " : " + description + " " + formatTimestamp(timestamp);
		}
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResultsDebug() {
		return getResultsBySeverity(Severity.DEBUG);
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResultsInformation() {
		return getResultsBySeverity(Severity.INFORMATION);
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResultsWarning() {
		return getResultsBySeverity(Severity.WARNING);
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResultsAverage() {
		return getResultsBySeverity(Severity.AVERAGE);
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResultsHigh() {
		return getResultsBySeverity(Severity.HIGH);
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResultsDisaster() {
		return getResultsBySeverity(Severity.DISASTER);
	}

	private List<TriggerResult> getResultsBySeverity(@Nullable Severity severity) {
		refresh();
		return cachedResults.entrySet().stream()
				.filter(entry -> entry.getKey().getSeverity() == severity)
				.map(entry -> new TriggerResult(
						null,
						entry.getKey().getName(),
						entry.getValue().description,
						entry.getValue().timestamp))
				.sorted(comparing(TriggerResult::getTimestamp))
				.collect(Collectors.toList());
	}

	@JmxAttribute
	synchronized public List<TriggerResult> getResults() {
		refresh();
		return cachedResults.entrySet().stream()
				.map(entry -> new TriggerResult(
						entry.getKey().getSeverity(),
						entry.getKey().getName(),
						entry.getValue().description,
						entry.getValue().timestamp))
				.sorted(comparing(TriggerResult::getSeverity).thenComparing(TriggerResult::getTimestamp))
				.collect(Collectors.toList());
	}

	@JmxAttribute
	synchronized public Severity getMaxSeverity() {
		refresh();
		return cachedResults.keySet().stream()
				.map(Trigger::getSeverity)
				.max(Comparator.naturalOrder())
				.orElse(null);
	}

	@JmxAttribute
	synchronized public String getMaxSeverityResult() {
		refresh();
		return cachedResults.entrySet().stream()
				.max(Comparator.comparing(entry -> entry.getKey().getSeverity()))
				.map(entry -> new TriggerResult(
						entry.getKey().getSeverity(),
						entry.getKey().getName(),
						entry.getValue().description,
						entry.getValue().timestamp))
				.map(Object::toString)
				.orElse(null);
	}

	@JmxAttribute
	synchronized public List<String> getTriggers() {
		return triggers.stream()
				.sorted(comparing(Trigger::getSeverity).reversed())
				.map(trigger -> trigger.getSeverity() + " : " + trigger.getName())
				.distinct()
				.collect(toList());
	}

	@JmxAttribute
	synchronized public String getTriggerNames() {
		return triggers.stream().map(Trigger::getName).distinct().collect(joining(", "));
	}

	@Override
	public String toString() {
		return getTriggerNames();
	}
}
