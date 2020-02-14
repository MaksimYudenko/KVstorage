package by.webapp.kvstorage.repository;

import by.webapp.kvstorage.model.Document;

import java.util.List;

public interface IDocumentRepository<T> {

    T save(T document);

    T findById(String id);

    int update(String key, T document);

    int delete(String key);

    List<Document> findAll();

}