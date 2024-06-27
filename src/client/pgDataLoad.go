package client

import (
	"database/sql"
	"fmt"
	"tpch_data_processor/tpch"
)

var createTablesChan chan bool

func LoadBaseData(cfg tpch.TpchConfigs, dbInfo DBInfo, completeChan chan bool) (tables *SQLTables) {
	createTablesChan = make(chan bool, 1)
	data := &tpch.TpchData{TpchConfigs: cfg}
	tables = &SQLTables{}
	data.PrepVars()
	fmt.Println("[PGDL]Finished prepVars()")
	data.FixTableEntries()
	fmt.Println("[PGDL]Finished FixTableEntries()")
	go ProcessBaseData(tables, data, false, completeChan) //false: not doing base data insertion into PostgreSQL
	data.ReadBaseData()
	fmt.Println("[PGDL]Finished ReadBaseData")
	return
}

func LoadAndSendBaseData(cfg tpch.TpchConfigs, dbInfo DBInfo, completeChan chan bool) (tables *SQLTables) {
	createTablesChan = make(chan bool, 1)
	data := &tpch.TpchData{TpchConfigs: cfg}
	tables = &SQLTables{}
	tables.SendDropTables(dbInfo)
	go tables.SendCreateTables(dbInfo)
	data.PrepVars()
	fmt.Println("[PGDL]Finished prepVars()")
	data.FixTableEntries()
	fmt.Println("[PGDL]Finished FixTableEntries()")
	go ProcessBaseData(tables, data, true, completeChan) //true: doing base data insertion into PostgreSQL
	go tables.SendInserts(dbInfo, data.ProcChan, completeChan)
	data.ReadBaseData()
	fmt.Println("[PGDL]Finished ReadBaseData")
	return
}

func ProcessBaseData(tables *SQLTables, data *tpch.TpchData, sendTables bool, completeChan chan bool) {
	for left := len(tpch.TableNames); left > 0; left-- {
		tableN := <-data.ReadChan
		fmt.Println("[PGDL]Creating", tpch.TableNames[tableN], tableN)
		tables.processTable(tableN, data)
		if sendTables {
			data.ProcChan <- tableN
		}
	}
	tables.InitConstants(false)
	fmt.Println("[PGGDL]Finished creating all tables.")
	if !sendTables {
		completeChan <- true
	}
	return
}

func (tables *SQLTables) processTable(tableN int, data *tpch.TpchData) {
	switch tableN {
	case CUSTOMER:
		tables.CreateCustomers(data.RawTables)
	case LINEITEM:
		tables.CreateLineitems(data.RawTables)
	case NATION:
		tables.CreateNations(data.RawTables)
	case ORDERS:
		tables.CreateOrders(data.RawTables)
	case PART:
		tables.CreateParts(data.RawTables)
	case REGION:
		tables.CreateRegions(data.RawTables)
	case PARTSUPP:
		tables.CreatePartsupps(data.RawTables)
	case SUPPLIER:
		tables.CreateSuppliers(data.RawTables)
	}
}

func (tables *SQLTables) SendDropTables(dbInfo DBInfo) {
	fmt.Println("[PGDL]Dropping LineItem table...")
	_, err := dbInfo.DB.NewDropTable().Model((*LineItem)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop LineItems table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Orders table...")
	_, err = dbInfo.DB.NewDropTable().Model((*Orders)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Orders table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Customer table...")
	_, err = dbInfo.DB.NewDropTable().Model((*Customer)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Customers table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping PartSupp table...")
	_, err = dbInfo.DB.NewDropTable().Model((*PartSupp)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop PartSupps table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Supplier table...")
	_, err = dbInfo.DB.NewDropTable().Model((*Supplier)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Suppliers table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Part table...")
	_, err = dbInfo.DB.NewDropTable().Model((*Part)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Parts table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Nation table...")
	_, err = dbInfo.DB.NewDropTable().Model((*Nation)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Nations table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Region table...")
	_, err = dbInfo.DB.NewDropTable().Model((*Region)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Regions table. Error:", err)
	}
}

func (tables *SQLTables) SendCreateTables(dbInfo DBInfo) {
	res, err := dbInfo.DB.NewCreateTable().Model((*Region)(nil)).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE REGION. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE REGION: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*Nation)(nil)).ForeignKey(`("n_regionkey") REFERENCES "regions" ("r_regionkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE NATION. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE NATION: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*Part)(nil)).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE PART. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE PART: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*Supplier)(nil)).ForeignKey(`("s_nationkey") REFERENCES "nations" ("n_nationkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE SUPPLIER. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE SUPPLIER: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*PartSupp)(nil)).ForeignKey(`("ps_suppkey") REFERENCES "suppliers" ("s_suppkey") ON DELETE CASCADE`).
		ForeignKey(`("ps_partkey") REFERENCES "parts" ("p_partkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE PARTSUPP. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE PARTSUPP: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*Customer)(nil)).ForeignKey(`("c_nationkey") REFERENCES "nations" ("n_nationkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE CUSTOMER. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE CUSTOMER: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*Orders)(nil)).ForeignKey(`("o_custkey") REFERENCES "customers" ("c_custkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE ORDERS. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE ORDERS: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*LineItem)(nil)).ForeignKey(`("l_orderkey") REFERENCES "orders" ("o_orderkey") ON DELETE CASCADE`).
		ForeignKey(`("l_partkey") REFERENCES "parts" ("p_partkey") ON DELETE CASCADE`).ForeignKey(`("l_suppkey") REFERENCES "suppliers" ("s_suppkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE LINEITEM. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE LINEITEM: %v\n", res)
	CreateTriggers(dbInfo)
	createTablesChan <- true
}

func (tables *SQLTables) SendInserts(dbInfo DBInfo, procChan chan int, completeChan chan bool) {
	nTables := len(tpch.TableNames)
	<-createTablesChan
	for i := 0; i < nTables; i++ {
		tableN := <-procChan
		if tableN == LINEITEM {
			continue //Skip; Do this one later after we get Parts and PartSupps
		}
		if tableN == PARTSUPP {
			continue //Skip; Do this one later after we get Parts
		}
		fmt.Println("[PGDL]Sending inserts for", getTableName(tableN))
		tables.sendInsertHelper(dbInfo, tableN)
	}
	fmt.Println("[PGDL]Sending inserts for", getTableName(PARTSUPP))
	tables.sendInsertHelper(dbInfo, PARTSUPP)
	fmt.Println("[PGDL]Sending inserts for", getTableName(LINEITEM))
	tables.sendInsertHelper(dbInfo, LINEITEM)
	fmt.Println("[PGDL]Finished all inserts.")
	testTable(dbInfo)
	completeChan <- true
}

func getTableName(tableN int) string {
	switch tableN {
	case CUSTOMER:
		return "customers"
	case LINEITEM:
		return "lineitems"
	case NATION:
		return "nations"
	case ORDERS:
		return "orders"
	case PART:
		return "parts"
	case PARTSUPP:
		return "partsupps"
	case SUPPLIER:
		return "suppliers"
	case REGION:
		return "regions"
	}
	return "oups."
}

func (tables *SQLTables) sendInsertHelper(dbInfo DBInfo, tableN int) {
	var res sql.Result
	var err error
	switch tableN {
	case CUSTOMER:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Customers).Exec(dbInfo.Ctx)
	case LINEITEM:
		if len(tables.LineItems) > 500000 {
			fmt.Println("[PGDL]Inserting line items in chunks")
			for i := 0; i < len(tables.LineItems); i += 500000 {
				end := i + 500000
				if end > len(tables.LineItems) {
					end = len(tables.LineItems)
				}
				part := tables.LineItems[i:end]
				res, err = dbInfo.DB.NewInsert().Model(&part).Exec(dbInfo.Ctx)
				if err != nil {
					fmt.Printf("[PGDL]Error on insert for part %d table %s. Error: %v\n", i, getTableName(tableN), err)
				}
				fmt.Printf("[PGDL]Result for insert for part %d table %s: %+v\n", i, getTableName(tableN), res)
			}
			return
		}
		res, err = dbInfo.DB.NewInsert().Model(&tables.LineItems).Exec(dbInfo.Ctx)
	case NATION:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Nations).Exec(dbInfo.Ctx)
	case ORDERS:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Orders).Exec(dbInfo.Ctx)
	case PART:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Parts).Exec(dbInfo.Ctx)
	case REGION:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Regions).Exec(dbInfo.Ctx)
		fmt.Printf("%v\n", tables.Regions)
	case PARTSUPP:
		res, err = dbInfo.DB.NewInsert().Model(&tables.PartSupps).Exec(dbInfo.Ctx)
	case SUPPLIER:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Suppliers).Exec(dbInfo.Ctx)
	}
	if err != nil {
		fmt.Printf("[PGDL]Error on insert for table %s. Error: %v\n", getTableName(tableN), err)
	}
	fmt.Printf("[PGDL]Result for insert for table %s: %+v\n", getTableName(tableN), res)
}

func testTable(dbInfo DBInfo) {
	var result []Region
	err := dbInfo.DB.NewSelect().Model((*Region)(nil)).Column("*").Scan(dbInfo.Ctx, &result)
	if err != nil {
		fmt.Println("[PGC]Error on the test table on regions:", err)
	}
	fmt.Println("[PGC]Test table:", result)
}
