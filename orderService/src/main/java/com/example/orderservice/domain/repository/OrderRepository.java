package com.example.orderservice.domain.repository;

import com.example.orderservice.domain.model.Order;
import com.example.orderservice.domain.model.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {

    List<Order> findByCustomerId(String customerId);

    Optional<Order> findByIdempotencyKey(String key);

    List<Order> findByStatus(OrderStatus status);

    /**
     * Loads the order with its items in a single JOIN FETCH query.
     * Use this instead of findById whenever items are needed outside a
     * @Transactional context to avoid LazyInitializationException.
     */
    @Query("SELECT o FROM Order o LEFT JOIN FETCH o.items WHERE o.id = :id")
    Optional<Order> findByIdWithItems(@Param("id") UUID id);

    /**
     * Loads all orders for a customer with items eagerly.
     * DISTINCT prevents duplicate Order rows when an order has multiple items.
     */
    @Query("SELECT DISTINCT o FROM Order o LEFT JOIN FETCH o.items WHERE o.customerId = :customerId")
    List<Order> findByCustomerIdWithItems(@Param("customerId") String customerId);
}
