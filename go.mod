module postgres_tpch_client

go 1.20

require (
	github.com/AndreRijo/go-tools v0.0.0-20240922171453-2d12c5769f8a
	github.com/uptrace/bun v1.2.3
	github.com/uptrace/bun/dialect/pgdialect v1.2.3
	github.com/uptrace/bun/driver/pgdriver v1.2.1
	postgres_tpch_go_lib v0.0.0-00010101000000-000000000000
	tpch_data_processor v0.0.0
)

require (
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.4.0 // indirect
	github.com/tmthrgd/go-hex v0.0.0-20190904060850-447a3041c3bc // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	mellium.im/sasl v0.3.1 // indirect
	potionDB/crdt v0.0.0 // indirect
	potionDB/shared v0.0.0 // indirect
)

replace tpch_data_processor v0.0.0 => ../tpch_data_processor

replace postgres_tpch_go_lib => ../postgresTpchGoLib

replace potionDB/crdt => ../potionDB/crdt

replace potionDB/shared => ../potionDB/shared
