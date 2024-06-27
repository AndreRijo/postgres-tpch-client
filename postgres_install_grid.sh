#Install Postgres
sudo-g5k
cd /home/arijo/private/Postgresql/postgresql-16.2/
sudo ./configure
sudo make
sudo make install
sudo chown arijo /usr/local/pgsql
mkdir -p /usr/local/pgsql/data
sudo chown arijo /usr/local/pgsql/data


#Start Postgres to create necessary files/folders and then stop it
/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data
/usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l logfile start
/usr/local/pgsql/bin/createdb test
/usr/local/pgsql/bin/pg_ctl stop -D /usr/local/pgsql/data

#Update pg_hba.conf in /usr/local/pgsql/data
cd /usr/local/pgsql/data
echo -e 'host \tall \t\tall \t\t0.0.0.0/0 \t\ttrust' >> pg_hba.conf

#Building extension
cd /home/arijo/private/pg_ivm-1.8
sudo make PG_CONFIG=/usr/local/pgsql/bin/pg_config install 

#Starting Postgres
cd /home/arijo/private
/usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -o "-c listen_addresses='*'" -l logfile start