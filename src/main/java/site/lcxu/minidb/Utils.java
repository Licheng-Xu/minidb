package site.lcxu.minidb;

public class Utils {
    public static void intToBytes(int value, byte[] buf, int offset) {
        buf[offset] = (byte) ((value & 0x000000FF));
        buf[offset + 1] = (byte) ((value & 0x0000FF00) >> 8);
        buf[offset + 2] = (byte) ((value & 0x00FF0000) >> 16);
        buf[offset + 3] = (byte) ((value & 0xFF000000) >> 24);
    }

    public static void shortToBytes(short value, byte[] buf, int offset) {
        buf[offset] = (byte) ((value & 0x000000FF));
        buf[offset + 1] = (byte) ((value & 0x0000FF00) >> 8);
    }

    public static int bytesToInt(byte[] buf, int offset) {
        return ((buf[offset] & 0xFF)
                | ((buf[offset + 1] << 8) & 0xFF00)
                | ((buf[offset + 2] << 16) & 0xFF0000)
                | ((buf[offset + 3] << 24) & 0xFF000000));
    }

    public static short bytesToShort(byte[] buf, int offset) {
        return (short) ((buf[offset] & 0xFF)
                | ((buf[offset + 1] << 8) & 0xFF00));
    }
}
