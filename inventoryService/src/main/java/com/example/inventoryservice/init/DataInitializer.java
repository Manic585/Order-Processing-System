package com.example.inventoryservice.init;

import com.example.inventoryservice.domain.model.Product;
import com.example.inventoryservice.domain.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Seeds the products table with sample data on first startup.
 * Only runs when the table is empty so that subsequent restarts are idempotent.
 *
 * Excluded from the "test" profile so integration tests can control their own fixtures.
 */
@Slf4j
@Component
@Profile("!test")
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;

    @Override
    public void run(String... args) {
        if (productRepository.count() > 0) {
            log.info("event=DATA_INIT_SKIP reason=products_table_not_empty");
            return;
        }

        List<Product> products = List.of(
                product("PROD001", "Laptop Pro 15",        100),
                product("PROD002", "Wireless Mouse",        100),
                product("PROD003", "Mechanical Keyboard",   100),
                product("PROD004", "USB-C Hub",             100),
                product("PROD005", "27-inch Monitor",       100)
        );

        products.forEach(p -> {
            if (!productRepository.existsById(p.getId())) {
                productRepository.save(p);
            }
        });
        //productRepository.saveAll(products);
        log.info("event=DATA_INIT_COMPLETE count={}", products.size());
    }

    private Product product(String id, String name, int qty) {
        return Product.builder()
                .id(id)
                .name(name)
                .availableQuantity(qty)
                .reservedQuantity(0)
                .version(0L)
                .build();
    }
}
