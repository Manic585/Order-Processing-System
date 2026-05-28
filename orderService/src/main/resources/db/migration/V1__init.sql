-- V1: Initial schema — orders and order_items

CREATE TABLE IF NOT EXISTS orders (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     VARCHAR(50)     NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    total_amount    DECIMAL(10, 2)  NOT NULL,
    idempotency_key VARCHAR(100)    NOT NULL,
    created_at      TIMESTAMP       NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP       NOT NULL DEFAULT now(),
    version         BIGINT          NOT NULL DEFAULT 0,

    CONSTRAINT orders_status_check CHECK (
        status IN ('PENDING','CONFIRMED','PROCESSING','SHIPPED','DELIVERED','CANCELLED')
    ),
    CONSTRAINT orders_idempotency_key_uq UNIQUE (idempotency_key)
);

CREATE TABLE IF NOT EXISTS order_items (
    id           UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id     UUID            NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id   VARCHAR(50)     NOT NULL,
    product_name VARCHAR(100)    NOT NULL,
    quantity     INTEGER         NOT NULL,
    unit_price   DECIMAL(10, 2)  NOT NULL,

    CONSTRAINT order_items_quantity_check CHECK (quantity > 0)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id     ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status          ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_idempotency_key ON orders (idempotency_key);
