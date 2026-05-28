package com.example.inventoryservice.domain.repository;

import com.example.inventoryservice.domain.model.Product;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, String> {

    /**
     * Fetches a product and acquires an optimistic lock.
     * Spring Data JPA will append FOR UPDATE in the generated SQL for
     * PESSIMISTIC modes; for OPTIMISTIC it simply reads and Hibernate
     * validates the version at flush time — preventing lost-update anomalies
     * without holding a DB row lock across the entire transaction.
     */
    @Lock(LockModeType.OPTIMISTIC)
    @Query("SELECT p FROM Product p WHERE p.id = :id")
    Optional<Product> findByIdWithLock(@Param("id") String id);

    /**
     * Bulk-fetch products by a list of IDs (used for read-only checks).
     */
    @Query("SELECT p FROM Product p WHERE p.id IN :ids")
    List<Product> findAllByIdIn(@Param("ids") List<String> ids);
}
