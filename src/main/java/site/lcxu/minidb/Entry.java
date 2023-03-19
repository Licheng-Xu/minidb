package site.lcxu.minidb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author licheng_xu
 * @date 2021/9/8
 */
public class Entry {
    public static final short PUT = 0;
    public static final short DEL = 1;
    public static final int ENTRY_HEADER_SIZE = 10;

    private byte[] key;
    private byte[] value;
    private int keySize;
    private int valueSize;
    private short mark;

    public Entry(int keySize, int valueSize, short mark) {
        this.key = new byte[keySize];
        this.value = new byte[valueSize];
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.mark = mark;
    }

    public Entry(byte[] key, byte[] value, short mark) {
        this.key = key;
        this.value = value;
        this.keySize = key.length;
        this.valueSize = value.length;
        this.mark = mark;
    }

    public int getSize() {
        return ENTRY_HEADER_SIZE + keySize + valueSize;
    }

    public byte[] encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.getSize());
        byteBuffer.putInt(keySize);
        byteBuffer.putInt(valueSize);
        byteBuffer.putShort(mark);
        byteBuffer.put(key);
        byteBuffer.put(value);
        return byteBuffer.array();
    }

    public static Entry decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Input buffer cannot be null");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keySize = byteBuffer.getInt();
        int valueSize = byteBuffer.getInt();
        short mark = byteBuffer.getShort();
        byte[] key = new byte[keySize];
        byteBuffer.get(key);
        byte[] value = new byte[valueSize];
        byteBuffer.get(value);
        return new Entry(key, value, mark);
    }

    public static Entry decodeHeader(byte[] header) {
        if (header == null) {
            throw new IllegalArgumentException("Input buffer cannot be null");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(header);
        int keySize = byteBuffer.getInt();
        int valueSize = byteBuffer.getInt();
        short mark = byteBuffer.getShort();
        return new Entry(keySize, valueSize, mark);
    }

    // override object method

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Entry entry = (Entry) o;
        return keySize == entry.keySize && valueSize == entry.valueSize && mark == entry.mark && Arrays.equals(key, entry.key) && Arrays.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(keySize, valueSize, mark);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Entry{" + "key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value) + ", keySize=" + keySize + ", valueSize=" + valueSize + ", mark=" + mark + '}';
    }

    // getter and setter

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getKeySize() {
        return keySize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public short getMark() {
        return mark;
    }
}
