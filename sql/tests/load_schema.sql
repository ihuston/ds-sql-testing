-- Sample schema to load
-- Get the real schema using something similar to the following:
-- pgdump

CREATE SCHEMA ds;

CREATE TABLE ds.test_table
(id int)
DISTRIBUTED BY (id);