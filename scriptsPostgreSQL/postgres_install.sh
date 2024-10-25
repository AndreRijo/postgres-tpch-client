#Install Postgres
#For the lines starting with "ALTER SYSTEM", you may want to change the values to better match your system's capabilities.
#$1: folder containing both PostgreSQL and pg_ivm
#$2: the location to install PostgreSQL (e.g., /usr/local)
#$3: your account's username

cd $1/Postgresql/postgresql-16.2/
sudo ./configure --prefix=$2/pgsql
sudo make
sudo make install
sudo chown $3 $2/pgsql
mkdir -p $2/pgsql/data
sudo chown $3 $2/pgsql/data


#Start Postgres to create necessary files/folders and then stop it
$2/pgsql/bin/pg_ctl initdb -D $2/pgsql/data
$2/pgsql/bin/pg_ctl -D $2/pgsql/data -l logfile start
$2/pgsql/bin/createdb test -h localhost
$2/pgsql/bin/psql test -h localhost -c "ALTER SYSTEM SET max_connections = 600;"
$2/pgsql/bin/psql test -h localhost -c "ALTER SYSTEM SET shared_buffers = 4194304;"
$2/pgsql/bin/psql test -h localhost -c "ALTER SYSTEM SET work_mem = 1048576;"
$2/pgsql/bin/psql test -h localhost -c "ALTER SYSTEM SET maintenance_work_mem = 4194304;"
$2/pgsql/bin/psql test -h localhost -c "ALTER SYSTEM SET synchronous_commit = off;"
$2/pgsql/bin/pg_ctl stop -D $2/pgsql/data

#Update pg_hba.conf in /usr/local/pgsql/data
cd $2/pgsql/data
echo -e 'host \tall \t\tall \t\t0.0.0.0/0 \t\ttrust' >> pg_hba.conf

#Building extension
cd $1/pg_ivm-1.8
sudo make PG_CONFIG=$2/pgsql/bin/pg_config install 

#Starting Postgres
cd $1
$2/pgsql/bin/pg_ctl -D $2/pgsql/data -o "-c listen_addresses='*'" -l logfile start
