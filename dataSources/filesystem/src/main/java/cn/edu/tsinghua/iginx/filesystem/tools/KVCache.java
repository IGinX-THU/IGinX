package cn.edu.tsinghua.iginx.filesystem.tools;

import org.checkerframework.checker.units.qual.K;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// 缓存任何数据，以减少重复性分析工作等
public class KVCache {
    private static int CAPACITY;
    private static AtomicInteger size;
    static ReentrantLock lock = new ReentrantLock();
    private static LinkedHashMap<Object, Object> cache;

    static {
        CAPACITY = 100_000;
        size=new AtomicInteger(0);
        LinkedHashMap<Object, Object> cache = new LinkedHashMap<Object, Object>(CAPACITY + 1, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) {
                return size() > CAPACITY;
            }
        };
    }

    public static void putKV(Object key, Object val) {
        lock.lock();
        cache.put(key,val);
        lock.unlock();
    }

   public static synchronized Object getV(Object key) {
        Object res = null;
       lock.lock();
       res  =cache.get(key);
       lock.unlock();
       return res;
   }

//    public synchronized void getOrDefault(Object key) {
//        lock.lock();
//        cache.putIfAbsent()
//        lock.unlock();
//    }
}
