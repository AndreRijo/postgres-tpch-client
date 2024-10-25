CREATE INDEX orderKeyI ON line_items (l_orderkey);
UPDATE orders SET o_sumquantity = (SELECT COUNT(*) FROM line_items WHERE orders.o_orderkey = l_orderkey);
--DROP INDEX orderKeyI;

ALTER TABLE regions ADD PRIMARY KEY (r_regionkey);
ALTER TABLE nations ADD PRIMARY KEY (n_nationkey);
ALTER TABLE nations ADD FOREIGN KEY (n_regionkey) REFERENCES regions(r_regionkey) ON DELETE CASCADE;
ALTER TABLE parts ADD PRIMARY KEY(p_partkey);
ALTER TABLE suppliers ADD PRIMARY KEY (s_suppkey);
ALTER TABLE suppliers ADD FOREIGN KEY (s_nationkey) REFERENCES nations(n_nationkey) ON DELETE CASCADE;
ALTER TABLE part_supps ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE part_supps ADD FOREIGN KEY (ps_partkey) REFERENCES parts(p_partkey) ON DELETE CASCADE;
ALTER TABLE part_supps ADD FOREIGN KEY (ps_suppkey) REFERENCES suppliers(s_suppkey) ON DELETE CASCADE;
ALTER TABLE customers ADD PRIMARY KEY (c_custkey);
ALTER TABLE customers ADD FOREIGN KEY (c_nationkey) REFERENCES nations(n_nationkey) ON DELETE CASCADE;
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);
ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customers(c_custkey) ON DELETE CASCADE;
--ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customers(c_custkey);
ALTER TABLE line_items ADD PRIMARY KEY (l_orderkey, l_partkey, l_suppkey, l_linenumber);
--ALTER TABLE line_items ADD FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey) ON DELETE CASCADE;
ALTER TABLE line_items ADD FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey);
ALTER TABLE line_items ADD FOREIGN KEY (l_partkey) REFERENCES parts(p_partkey) ON DELETE CASCADE;
--ALTER TABLE line_items ADD FOREIGN KEY (l_partkey) REFERENCES parts(p_partkey);
ALTER TABLE line_items ADD FOREIGN KEY (l_suppkey) REFERENCES suppliers(s_suppkey) ON DELETE CASCADE;
--ALTER TABLE line_items ADD FOREIGN KEY (l_suppkey) REFERENCES suppliers(s_suppkey);
CREATE OR REPLACE FUNCTION update_sum_quantity() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$BEGIN UPDATE orders SET o_sumquantity = o_sumquantity + NEW.l_quantity WHERE o_orderkey = NEW.l_orderkey; RETURN NEW; END; $$;
CREATE TRIGGER trigger_update_sum_quantity AFTER INSERT ON line_items FOR EACH ROW EXECUTE FUNCTION update_sum_quantity();

#Not needed, client creates these only when materialized views are not employed.
#CREATE INDEX shipdate ON line_items (l_shipdate);
#CREATE INDEX orderdateyear ON orders (EXTRACT(year FROM o_orderdate));
#CREATE INDEX suppliernation ON suppliers (s_nationkey);
#CREATE INDEX suppkey ON part_supps (ps_suppkey);
#CREATE INDEX partkey ON part_supps (ps_partkey);

CREATE VIEW revenue19931 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1993-01-01' AND l_shipdate <= date '1993-03-31'
GROUP BY l_suppkey;
CREATE VIEW revenue19934 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1993-04-01' AND l_shipdate <= date '1993-06-30'
GROUP BY l_suppkey;
CREATE VIEW revenue19937 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1993-07-01' AND l_shipdate <= date '1993-09-30'
GROUP BY l_suppkey;
CREATE VIEW revenue199310 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1993-10-01' AND l_shipdate <= date '1993-12-31'
GROUP BY l_suppkey;

CREATE VIEW revenue19941 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1994-01-01' AND l_shipdate <= date '1994-03-31'
GROUP BY l_suppkey;
CREATE VIEW revenue19944 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1994-04-01' AND l_shipdate <= date '1994-06-30'
GROUP BY l_suppkey;
CREATE VIEW revenue19947 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1994-07-01' AND l_shipdate <= date '1994-09-30'
GROUP BY l_suppkey;
CREATE VIEW revenue199410 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1994-10-01' AND l_shipdate <= date '1994-12-31'
GROUP BY l_suppkey;

CREATE VIEW revenue19951 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1995-01-01' AND l_shipdate <= date '1995-03-31'
GROUP BY l_suppkey;
CREATE VIEW revenue19954 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1995-04-01' AND l_shipdate <= date '1995-06-30'
GROUP BY l_suppkey;
CREATE VIEW revenue19957 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1995-07-01' AND l_shipdate <= date '1995-09-30'
GROUP BY l_suppkey;
CREATE VIEW revenue199510 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1995-10-01' AND l_shipdate <= date '1995-12-31'
GROUP BY l_suppkey;

CREATE VIEW revenue19961 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1996-01-01' AND l_shipdate <= date '1996-03-31'
GROUP BY l_suppkey;
CREATE VIEW revenue19964 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1996-04-01' AND l_shipdate <= date '1996-06-30'
GROUP BY l_suppkey;
CREATE VIEW revenue19967 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1996-07-01' AND l_shipdate <= date '1996-09-30'
GROUP BY l_suppkey;
CREATE VIEW revenue199610 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1996-10-01' AND l_shipdate <= date '1996-12-31'
GROUP BY l_suppkey;

CREATE VIEW revenue19971 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1997-01-01' AND l_shipdate <= date '1997-03-31'
GROUP BY l_suppkey;
CREATE VIEW revenue19974 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1997-04-01' AND l_shipdate <= date '1997-06-30'
GROUP BY l_suppkey;
CREATE VIEW revenue19977 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1997-07-01' AND l_shipdate <= date '1997-09-30'
GROUP BY l_suppkey;
CREATE VIEW revenue199710 (supplier_no, total_revenue) as
SELECT l_suppkey, sum(l_extendedprice * (1-l_discount))
FROM line_items
WHERE l_shipdate >= date '1997-10-01' AND l_shipdate <= date '1997-12-31'
GROUP BY l_suppkey;
