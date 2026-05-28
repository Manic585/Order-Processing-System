-- inventory-service V1 migration
-- Flyway is scoped to the 'inventory' schema, so all objects land there.

CREATE TABLE IF NOT EXISTS inventory.products (
    id                  VARCHAR(50)    NOT NULL,
    name                VARCHAR(100)   NOT NULL,
    available_quantity  INTEGER        NOT NULL DEFAULT 0,
    reserved_quantity   INTEGER        NOT NULL DEFAULT 0,
    version             BIGINT         NOT NULL DEFAULT 0,

    CONSTRAINT pk_products          PRIMARY KEY (id),
    CONSTRAINT chk_available_qty    CHECK (available_quantity >= 0),
    CONSTRAINT chk_reserved_qty     CHECK (reserved_quantity  >= 0)
);

CREATE INDEX IF NOT EXISTS idx_products_name ON inventory.products (name);
