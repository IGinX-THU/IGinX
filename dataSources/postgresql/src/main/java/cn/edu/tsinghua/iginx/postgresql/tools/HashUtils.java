package cn.edu.tsinghua.iginx.postgresql.tools;

public class HashUtils {

    public static long toHash(String s) {
        char c[] = s.toCharArray();
        long hv = 0;
        long base = 131;
        for (int i = 0; i < c.length; i++) {
            hv = hv * base + (long) c[i];   //利用自然数溢出，即超过 LONG_MAX 自动溢出，节省时间
        }
        if (hv < 0) {
            return -1 * hv;
        }
        return hv;
    }
}
