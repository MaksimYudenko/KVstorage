package by.webapp.kvstorage.service;

import java.util.List;

public interface GlobalService<T> {

    T create(T entity);

    T get(String entityId);

    int update(String entityId, T entity);

    int delete(String entityId);

    List<T> list();

}