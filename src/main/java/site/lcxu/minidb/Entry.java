package site.lcxu.minidb;

import java.util.Arrays;

public class Entry {
    public static final short PUT = 0;
    public static final short DEL = 1;
    public static final int ENTRY_HEADER_SIZE = 10;

    public byte[] key;
    public byte[] value;
    public int keySize;
    public int valueSize;
    public short mark;

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

    @Override
    public String toString() {
        return "Entry{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", keySize=" + keySize +
                ", valueSize=" + valueSize +
                ", mark=" + mark +
                '}';
    }

    // 这里有个妥协，java 数组长度限制为 int32，如果返回 long 就不能编码成 byte 数组了
    public int getSize() {
        return ENTRY_HEADER_SIZE + keySize + valueSize;
    }

    public byte[] encode() {
        byte[] buf = new byte[this.getSize()];
        Utils.intToBytes(keySize, buf, 0);
        Utils.intToBytes(valueSize, buf, 4);
        Utils.shortToBytes(mark, buf, 8);
        System.arraycopy(key, 0, buf, 10, keySize);
        System.arraycopy(value, 0, buf, 10 + keySize, valueSize);
        return buf;
    }

    public static Entry decode(byte[] buf) {
        int keySize = Utils.bytesToInt(buf, 0);
        int valueSize = Utils.bytesToInt(buf, 4);
        short mark = Utils.bytesToShort(buf, 8);
        return new Entry(keySize, valueSize, mark);
    }

    public static void main(String[] args) {
        byte[] key = {1, 2};
        byte[] value = {4, 5, 6};
        Entry entry = new Entry(key, value, (short) 1);
        byte[] encoded = entry.encode();
        System.out.println(Arrays.toString(encoded));
        Entry entryCopy = decode(encoded);
        System.out.println(entryCopy);
    }
}
