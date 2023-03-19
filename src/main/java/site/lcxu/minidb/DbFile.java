package site.lcxu.minidb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Logger;

/**
 * @author licheng_xu
 * @date 2021/9/8
 */
public class DbFile {
    public static final String FILE_NAME = "minidb.data";
    public static final String MERGE_FILE_NAME = "minidb.data.merge";

    private final Logger logger = Logger.getLogger(getClass().getName());

    public File file;
    public long offset;

    private DbFile(String fileName) throws IOException {
        File file = new File(fileName);
        if (!file.createNewFile()) {
            logger.warning("db file is already existed.");
        }
        this.file = file;
        this.offset = (int) file.length();
    }

    public DbFile(String path, boolean isMergeFile) throws IOException {
        this(path + File.separator + (isMergeFile ? MERGE_FILE_NAME : FILE_NAME));
    }

    public Entry read(long offset) throws Exception {
        byte[] buf = new byte[Entry.ENTRY_HEADER_SIZE];
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(offset);
        if (randomAccessFile.read(buf) == -1) {
            // 读取到文件末尾
            throw new IndexOutOfBoundsException();
        }
        Entry entry = Entry.decodeHeader(buf);

        offset += Entry.ENTRY_HEADER_SIZE;
        if (entry.getKeySize() > 0) {
            byte[] key = new byte[entry.getKeySize()];
            randomAccessFile.seek(offset);
            randomAccessFile.read(key);
            entry.setKey(key);
        }

        offset += entry.getKeySize();
        if (entry.getValueSize() > 0) {
            byte[] value = new byte[entry.getValueSize()];
            randomAccessFile.seek(offset);
            randomAccessFile.read(value);
            entry.setValue(value);
        }

        randomAccessFile.close();
        return entry;
    }

    public void write(Entry entry) throws IOException {
        byte[] encoded = entry.encode();
        FileOutputStream output = new FileOutputStream(file, true);
        output.write(encoded);
        output.close();
        this.offset += entry.getSize();
    }

}
