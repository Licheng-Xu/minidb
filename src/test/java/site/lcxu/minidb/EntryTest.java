package site.lcxu.minidb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author licheng_xu
 * @date 2023/3/19
 */
class EntryTest {
    Entry entry;
    byte[] bytes;

    @BeforeEach
    void setUp() {
        byte[] key = {1, 2};
        byte[] value = {4, 5, 6};
        entry = new Entry(key, value, (short) 1);
        bytes = new byte[]{0, 0, 0, 2, 0, 0, 0, 3, 0, 1, 1, 2, 4, 5, 6};
    }

    @Test
    void testToString() {
        String wantStr = "Entry{key=[1, 2], value=[4, 5, 6], keySize=2, valueSize=3, mark=1}";
        assertEquals(wantStr, entry.toString());
    }

    @Test
    void getSize() {
        assertEquals(15, entry.getSize());
    }

    @Test
    void encode() {
        assertArrayEquals(bytes, entry.encode());
    }

    @Test
    void decode() {
        assertEquals(entry, Entry.decode(bytes));
    }

    @Test
    void decodeHeader() {
        Entry newEntry = Entry.decodeHeader(bytes);
        assertEquals(entry.getKeySize(), newEntry.getKeySize());
        assertEquals(entry.getValueSize(), newEntry.getValueSize());
        assertEquals(entry.getMark(), newEntry.getMark());
    }
}