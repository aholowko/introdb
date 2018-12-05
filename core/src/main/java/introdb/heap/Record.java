package introdb.heap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * marker     - 1 byte
 * keySize    - 4 bytes
 * key
 * value size - 4 bytes
 * value
 */
class Record {

    //    1 byte (marker)
    // +  4 bytes (keySize)
    // + 4 bytes (valueSize)
    // ======================
    // = 9 bytes
    private static final byte ATTRIBUTES_SIZE = 1 + Integer.BYTES * 2;

    private static final byte ALIVE = 0x1;
    private static final byte TOMBSTONE = 0xf;

    private byte marker = ALIVE;
    private final int recordSize;
    private final int keySize;
    private final byte[] key;
    private final int valueSize;
    private final byte[] value;

    private Record(byte[] key, byte[] value) {
        this(ALIVE, key, value);
    }

    private Record(byte marker, byte[] key, byte[] value) {
        this.marker = marker;
        this.keySize = key.length;
        this.valueSize = value.length;
        this.recordSize = recordSize(keySize, valueSize);
        this.key = key;
        this.value = value;
    }

    static Record of(Entry entry) throws IOException {
        return new Record(
                serialize(entry.key()),
                serialize(entry.value())
        );
    }

    static byte[] serialize(Serializable value) throws IOException {
        final var baos = new ByteArrayOutputStream();
        try (final var objectOutput = new ObjectOutputStream(baos)) {
            objectOutput.writeObject(value);
        }
        return baos.toByteArray();
    }

    void markAsRemoved() {
        this.marker = TOMBSTONE;
    }

    private Serializable deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInput in = new ObjectInputStream(bis)) {
            return (Serializable) in.readObject();
        }
    }

    Entry entry() throws IOException, ClassNotFoundException {
        return new Entry(deserialize(key), deserialize(value));
    }

    private int recordSize(int keySize, int valueSize) {
        return ATTRIBUTES_SIZE + keySize + valueSize;
    }

    int size() {
        return recordSize;
    }

    byte[] key() {
        return key;
    }

    Object valueDeserialized() throws IOException, ClassNotFoundException {
        return deserialize(value);
    }

    void writeTo(ByteBuffer bb) {
        if (bb.remaining() < size()) {
            throw new IllegalStateException("No space left in the page");
        }
        bb.put(marker);         //1 byte
        bb.putInt(keySize);     //4 bytes
        bb.put(key);            //keySize bytes
        bb.putInt(valueSize);   //4 bytes
        bb.put(value);          //valueSize bytes
    }

    static Record readFrom(ByteBuffer bb) {
        if (!bb.hasRemaining()) {
            return null;
        }
        final byte marker = bb.get();
        if (marker == 0) {
            return null;
        }
        final int keySize = bb.getInt();
        final byte[] key = new byte[keySize];
        bb.get(key);
        final int valueSize = bb.getInt();
        final byte[] value = new byte[valueSize];
        bb.get(value);
        return new Record(marker, key, value);
    }

    public boolean isAlive() {
        return marker == ALIVE;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Record.class.getSimpleName() + "[", "]")
                .add("alive=" + isAlive())
                .add("recordSize=" + recordSize)
                .add("keySize=" + keySize)
                .add("key=" + Arrays.toString(key))
                .add("valueSize=" + valueSize)
                .add("value=" + Arrays.toString(value))
                .toString();
    }
}
