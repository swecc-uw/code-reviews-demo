package io;

public interface KVDAO<K, V, T> {
    void batchPut(T[] items);

    void put(K key, V value);

    V[] batchGet(K[] keys);

    V get(K key);

    V[] batchDelete(K[] keys);

    void delete(K key);

    void close();
}
