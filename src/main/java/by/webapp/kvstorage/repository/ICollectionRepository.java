package by.webapp.kvstorage.repository;

import by.webapp.kvstorage.model.Collection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ICollectionRepository extends JpaRepository<Collection, String> {
}