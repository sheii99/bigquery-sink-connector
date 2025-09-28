package com.iblync.kafka.connect.bigquery.sink;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class ConcurrencyHint {

	public static ConcurrencyHint of(Object value) {
		return new ConcurrencyHint(value);
	}

	public static ConcurrencyHint alwaysConcurrent() {
		return ALWAYS_CONCURRENT;
	}

	public static ConcurrencyHint neverConcurrent() {
		return NEVER_CONCURRENT;
	}

	private final Object value;

	private static final ConcurrencyHint NEVER_CONCURRENT = new ConcurrencyHint(new Object()) {
		@Override
		public String toString() {
			return "ConcurrencyHint.NEVER_CONCURRENT";
		}
	};

	private static final ConcurrencyHint ALWAYS_CONCURRENT = new ConcurrencyHint(new Object()) {
		@Override
		public String toString() {
			return "ConcurrencyHint.ALWAYS_CONCURRENT";
		}
	};

	private ConcurrencyHint(Object value) {
		this.value = requireNonNull(value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ConcurrencyHint that = (ConcurrencyHint) o;
		return value.equals(that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}

	@Override
	public String toString() {
		return "ConcurrencyHint{" + "value=" + value + '}';
	}
}
