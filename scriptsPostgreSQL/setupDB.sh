#$1: location of psql
#$2: location of scripts
#$3: location of tables (with SF)

#./
#/Users/a.rijo/Documents/University_11th_year/PostgreSQL/
#/Users/a.rijo/Documents/University_6th_year/potionDB_docs/tpch_data/tables/0.1SF/
#/usr/local/pgsql/bin/
#/home/arijo/private/
#/home/arijo/private/tpch_data/tables/1SF/


echo $1
echo $2
echo $3
$1psql -d test -f $2createTables.sql

cat $3region.tbl | cut -d '|' -f1-3 | $1psql -h localhost test -c "COPY regions FROM STDIN WITH DELIMITER '|';"
cat $3nation.tbl | cut -d '|' -f1-4 | $1psql -h localhost test -c "COPY nations FROM STDIN WITH DELIMITER '|';"
cat $3part.tbl | cut -d '|' -f1-9 | $1psql -h localhost test -c "COPY parts FROM STDIN WITH DELIMITER '|';"
cat $3supplier.tbl | cut -d '|' -f1-7 | $1psql -h localhost test -c "COPY suppliers FROM STDIN WITH DELIMITER '|';"
cat $3partsupp.tbl | cut -d '|' -f1-5 | $1psql -h localhost test -c "COPY part_supps FROM STDIN WITH DELIMITER '|';"
cat $3customer.tbl | cut -d '|' -f1-8 | $1psql -h localhost test -c "COPY customers FROM STDIN WITH DELIMITER '|';"
cat $3orders.tbl | cut -d '|' -f1-9 | $1psql -h localhost test -c "COPY orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) FROM STDIN WITH DELIMITER '|';"
cat $3lineitem.tbl | cut -d '|' -f1-16 | $1psql -h localhost test -c "COPY line_items FROM STDIN WITH DELIMITER '|';"

$1psql -h localhost -d test -f $2alterTables.sql
