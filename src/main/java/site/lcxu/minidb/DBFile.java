package site.lcxu.minidb;

import java.io.*;

public class DBFile {
    public static final String FILE_NAME = "minidb.data";
    public static final String MERGE_FILE_NAME = "minidb.data.merge";


    public File file;
    public long offset;

    private DBFile(String fileName) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.file = file;
        this.offset = (int) file.length();
    }

    public DBFile(String path, boolean isMergeFile) throws IOException {
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
        Entry entry = Entry.decode(buf);

        offset += Entry.ENTRY_HEADER_SIZE;
        if (entry.keySize > 0) {
            byte[] key = new byte[entry.keySize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(key);
            entry.key = key;
        }

        offset += entry.keySize;
        if (entry.valueSize > 0) {
            byte[] value = new byte[entry.valueSize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(value);
            entry.value = value;
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
