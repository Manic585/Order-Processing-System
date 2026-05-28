-- V2: Products table for the inventory-service domain
--     Placed in the 'inventory' schema to keep concerns separate.

CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.products (
    id                 VARCHAR(50)  PRIMARY KEY,
    name               VARCHAR(100) NOT NULL,
    available_quantity INTEGER      NOT NULL DEFAULT 0,
    reserved_quantity  INTEGER      NOT NULL DEFAULT 0,
    version            BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT products_available_quantity_check CHECK (available_quantity >= 0),
    CONSTRAINT products_reserved_quantity_check  CHECK (reserved_quantity  >= 0)
);
