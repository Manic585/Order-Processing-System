-- V3: Add FAILED to the orders status check constraint.
--     Required for placeOrder compensation when Kafka publish fails.

ALTER TABLE orders DROP CONSTRAINT orders_status_check;

ALTER TABLE orders ADD CONSTRAINT orders_status_check CHECK (
    status IN ('PENDING','CONFIRMED','PROCESSING','SHIPPED','DELIVERED','CANCELLED','FAILED')
);
