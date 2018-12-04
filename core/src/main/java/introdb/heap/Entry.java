package introdb.heap;

import java.io.Serializable;
import java.util.Objects;

class Entry {

	private final Serializable key;
	private final Serializable value;

	Entry(Serializable key, Serializable value) {
		if (key == null || value == null) {
			throw new IllegalArgumentException();
		}
		this.key = key;
		this.value = value;
	}

	Serializable key() {
		return key;
	}

	Serializable value() {
		return value;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Entry [key=").append(key).append(", value=").append(value).append("]");
		return builder.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Entry entry = (Entry) o;
		return key.equals(entry.key) &&
				value.equals(entry.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}
}
