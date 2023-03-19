package site.lcxu.minidb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author licheng_xu
 * @date 2021/9/8
 */
public class Database {
    /**
     * 内存中的索引信息
     */
    private final Map<String, Long> indexes;

    /**
     * 数据文件
     */
    private DbFile dbFile;

    /**
     * 数据目录
     */
    private final String dirPath;

    /**
     * 读写锁
     */
    private final ReadWriteLock lock;

    public Database(String dirPath) throws Exception {
        File dir = new File(dirPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new Exception("Can't create the directory!");
        }

        this.dbFile = new DbFile(dirPath, false);
        this.dirPath = dirPath;

        this.indexes = new HashMap<>();
        this.loadIndexesFromFile(dbFile);

        this.lock = new ReentrantReadWriteLock();
    }

    private void loadIndexesFromFile(DbFile dbFile) throws Exception {
        if (dbFile == null) {
            return;
        }

        long offset = 0;
        while (true) {
            Entry entry;
            try {
                entry = dbFile.read(offset);
            } catch (Exception e) {
                if (e instanceof IndexOutOfBoundsException) {
                    break;
                }
                throw e;
            }
            if (entry.getMark() == Entry.PUT) {
                indexes.put(Arrays.toString(entry.getKey()), offset);
            }
            offset += entry.getSize();
        }
    }

    public void merge() throws Exception {
        if (this.dbFile.offset == 0) {
            return;
        }

        List<Entry> validEntries = new ArrayList<>();
        long offset = 0;
        while (true) {
            Entry entry;
            try {
                entry = this.dbFile.read(offset);
            } catch (Exception e) {
                if (e instanceof IndexOutOfBoundsException) {
                    break;
                }
                throw e;
            }
            Long off = this.indexes.get(Arrays.toString(entry.getKey()));
            if (off != null && off == offset) {
                validEntries.add(entry);
            }
            offset += entry.getSize();
        }

        if (validEntries.size() > 0) {
            DbFile mergeDbFile = new DbFile(this.dirPath, true);
            for (Entry entry : validEntries) {
                long writeOff = mergeDbFile.offset;
                mergeDbFile.write(entry);
                this.indexes.put(Arrays.toString(entry.getKey()), writeOff);
            }
            this.dbFile.file.delete();
            mergeDbFile.file.renameTo(this.dbFile.file);
            this.dbFile = mergeDbFile;
        }

    }

    public void put(byte[] key, byte[] value) throws IOException {
        if (key.length == 0) {
            return;
        }

        this.lock.writeLock().lock();
        try {
            long offset = this.dbFile.offset;
            Entry entry = new Entry(key, value, Entry.PUT);
            this.dbFile.write(entry);
            this.indexes.put(Arrays.toString(key), offset);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public byte[] get(byte[] key) throws Exception {
        if (key.length == 0) {
            return null;
        }

        this.lock.readLock().lock();
        try {
            long offset = this.indexes.get(Arrays.toString(key));
            Entry entry = this.dbFile.read(offset);
            if (entry != null) {
                return entry.getValue();
            }
            return null;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void del(byte[] key) throws IOException {
        if (key.length == 0) {
            return;
        }

        this.lock.writeLock().lock();
        try {
            Entry entry = new Entry(key, new byte[0], Entry.DEL);
            this.dbFile.write(entry);
            this.indexes.remove(Arrays.toString(key));
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public static void main(String[] args) throws Exception {
        Database database = new Database("C:\\Users\\licheng_xu\\Documents\\Projects\\Java\\minidb");
        String[] pairs = {
                "k1", "val1",
                "k2", "val2",
                "k3", "val3"
        };
        database.put(pairs[0].getBytes(), pairs[1].getBytes());
        database.put(pairs[2].getBytes(), pairs[3].getBytes());
        database.put(pairs[4].getBytes(), pairs[5].getBytes());

        byte[] value3 = database.get(pairs[2].getBytes());
        System.out.println(Arrays.toString(value3));

        database.del(pairs[2].getBytes());
        database.merge();

    }
}
