DROP TABLE IF EXISTS line_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS part_supps CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS parts CASCADE;
DROP TABLE IF EXISTS nations CASCADE;
DROP TABLE IF EXISTS regions CASCADE;

CREATE TABLE regions (r_regionkey smallint, r_name character varying, r_comment character varying);
CREATE TABLE nations (n_nationkey smallint, n_name character varying, n_regionkey smallint, n_comment character varying);
CREATE TABLE parts (p_partkey integer, p_name character varying, p_mfgr character varying, p_brand character varying, p_type character varying, p_size character varying, p_container character varying, p_retailprice character varying, p_comment character varying);
CREATE TABLE suppliers (s_suppkey integer, s_name character varying, s_address character varying, s_nationkey smallint, s_phone character varying, s_acctbal character varying, s_comment character varying);
CREATE TABLE part_supps (ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost double precision, ps_comment character varying);
CREATE TABLE customers (c_custkey integer, c_name character varying, c_address character varying, c_nationkey smallint, c_phone character varying, c_acctbal character varying, c_mktsegment character varying, c_comment character varying);
CREATE TABLE orders (o_orderkey integer, o_custkey integer, o_orderstatus character varying, o_totalprice character varying, o_orderdate timestamp without time zone, o_orderpriority character varying, o_clerk character varying, o_shippriority character varying, o_comment character varying, o_sumquantity integer);
CREATE TABLE line_items (l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber smallint, l_quantity smallint, l_extendedprice double precision, l_discount double precision, l_tax double precision, l_returnflag character varying, l_linestatus character varying, l_shipdate timestamp without time zone, l_commitdate timestamp without time zone, l_receiptdate timestamp without time zone, l_shipinstruct character varying, l_shipmode character varying, l_comment character varying);
