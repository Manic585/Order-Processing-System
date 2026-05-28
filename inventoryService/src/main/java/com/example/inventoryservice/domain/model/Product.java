package com.example.inventoryservice.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "products", schema = "inventory")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    @Id
    @Column(nullable = false, length = 50)
    private String id;

    @Column(nullable = false, length = 100)
    private String name;

    @Column(name = "available_quantity", nullable = false)
    private Integer availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private Integer reservedQuantity;

    /**
     * Optimistic-locking version. Hibernate increments this on every UPDATE.
     * A concurrent modification raises OptimisticLockException, which is a
     * subtype of RuntimeException and will trigger a transaction rollback.
     */
    @Version
    @Column(nullable = false)
    private Long version;
}
