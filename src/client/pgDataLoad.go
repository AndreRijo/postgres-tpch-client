package client

import (
	"database/sql"
	"fmt"
	"postgres_tpch_go_lib/src/proto"
	pgTpch "postgres_tpch_go_lib/src/tpch"
	"strconv"
	"time"
	"tpch_data_processor/tpch"
)

/*type UpdateData struct {
	OrderUpds, LineItemUpds          [][]string
	DeleteKeys                       []string
	LineItemSizes, ItemSizesPerOrder []int
}*/

type UpdateData struct {
	//TableInfos                  []tpch.TableInfo
	RoutineOrders, RoutineItems [][][]string
	RoutineDeletes              [][]int32
	RoutineLineSizes            [][]int
	RegionPerClient             []int
}

var (
	createTablesChan, tablesReadyChan chan bool
	UpdParts                          = [...]int{9, 16} //Number of (original) fields of Order, LineItem
	N_ORDER_FIELDS, N_ITEM_FIELDS     = 9, 16           //Will be updated to the actual values when reading the tables
	data                              *tpch.TpchData
)

func LoadBaseData(cfg tpch.TpchConfigs, dbInfo DBInfo, completeChan chan bool, updateChan chan UpdateData) (tables *pgTpch.SQLTables) {
	createTablesChan = make(chan bool, 1)
	data = &tpch.TpchData{TpchConfigs: cfg}
	tables, tablesReadyChan = &pgTpch.SQLTables{}, make(chan bool, 1)
	data.PrepVars()
	fmt.Println("[PGDL]Finished prepVars()")
	data.FixTableEntries()
	fmt.Println("[PGDL]Finished FixTableEntries()")
	data.ReadHeaders()
	N_ORDER_FIELDS, N_ITEM_FIELDS = len(data.ToRead[tpch.ORDERS]), len(data.ToRead[tpch.LINEITEM])
	if DOES_UPDATES && UPDATE_RATE > 0 {
		go ReadUpdates(data, tables, updateChan)
	}
	go ProcessBaseData(tables, data, false, completeChan) //false: not doing base data insertion into PostgreSQL
	data.ReadBaseData()
	fmt.Println("[PGDL]Finished ReadBaseData")
	return
}

func LoadAndSendBaseData(cfg tpch.TpchConfigs, dbInfo DBInfo, completeChan chan bool, updateChan chan UpdateData) (tables *pgTpch.SQLTables) {
	createTablesChan = make(chan bool, 1)
	data := &tpch.TpchData{TpchConfigs: cfg}
	tables = &pgTpch.SQLTables{}
	SendDropTables(dbInfo)
	go SendCreateTables(dbInfo, tables)
	data.PrepVars()
	fmt.Println("[PGDL]Finished prepVars()")
	data.FixTableEntries()
	fmt.Println("[PGDL]Finished FixTableEntries()")
	data.ReadHeaders()
	N_ORDER_FIELDS, N_ITEM_FIELDS = len(data.ToRead[tpch.ORDERS]), len(data.ToRead[tpch.LINEITEM])
	if DOES_UPDATES && UPDATE_RATE > 0 {
		go ReadUpdates(data, tables, updateChan)
	}
	go ProcessBaseData(tables, data, true, completeChan) //true: doing base data insertion into PostgreSQL
	go SendInserts(dbInfo, data.ProcChan, completeChan, tables)
	data.ReadBaseData()
	fmt.Println("[PGDL]Finished ReadBaseData")
	return
}

func LoadAndSendBaseDataRedirect(cfg tpch.TpchConfigs, dbInfo DBInfo, completeChan chan bool) (tables *pgTpch.SQLTables) {
	createTablesChan = make(chan bool, 1)
	data := &tpch.TpchData{TpchConfigs: cfg}
	tables = &pgTpch.SQLTables{}
	SendDropTablesRedirect(dbInfo)
	go SendCreateTablesRedirect(dbInfo, tables)
	data.PrepVars()
	fmt.Println("[PGDL]Finished prepVars()")
	data.FixTableEntries()
	fmt.Println("[PGDL]Finished FixTableEntries()")
	go ProcessBaseData(tables, data, true, completeChan) //true: doing base data insertion into PostgreSQL
	go SendInsertsRedirect(dbInfo, data.ProcChan, completeChan, tables)
	data.ReadBaseData()
	fmt.Println("[PGDL]Finished ReadBaseData")
	return

}

func ProcessBaseData(tables *pgTpch.SQLTables, data *tpch.TpchData, sendTables bool, completeChan chan bool) {
	for left := len(tpch.TableNames); left > 0; left-- {
		tableN := <-data.ReadChan
		fmt.Println("[PGDL]Creating", tpch.TableNames[tableN], tableN)
		processTable(tableN, data, tables)
		if sendTables {
			data.ProcChan <- tableN
		}
	}
	fmt.Println("[PGGDL]Finished creating all tables.")
	if !sendTables {
		completeChan <- true
	}
	return
}

func processTable(tableN int, data *tpch.TpchData, tables *pgTpch.SQLTables) {
	switch tableN {
	case pgTpch.CUSTOMER:
		tables.CreateCustomers(data.RawTables)
		//When Customers are read, so are nations and regions - thus, we can start processing updates.
		tablesReadyChan <- true
	case pgTpch.LINEITEM:
		tables.CreateLineitems(data.RawTables)
	case pgTpch.NATION:
		tables.CreateNations(data.RawTables)
		tables.InitConstants(false) //Can init constants here. We'll need them for processing updates
	case pgTpch.ORDERS:
		tables.CreateOrders(data.RawTables)
	case pgTpch.PART:
		tables.CreateParts(data.RawTables)
	case pgTpch.REGION:
		tables.CreateRegions(data.RawTables)
	case pgTpch.PARTSUPP:
		tables.CreatePartsupps(data.RawTables)
	case pgTpch.SUPPLIER:
		tables.CreateSuppliers(data.RawTables)
	}
}

func SendDropTables(dbInfo DBInfo) {
	fmt.Println("[PGDL]Dropping LineItem table...")
	_, err := dbInfo.DB.NewDropTable().Model((*pgTpch.LineItem)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop LineItems table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Orders table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.Orders)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Orders table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Customer table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.Customer)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Customers table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping PartSupp table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.PartSupp)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop PartSupps table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Supplier table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.Supplier)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Suppliers table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Part table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.Part)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Parts table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Nation table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.Nation)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Nations table. Error:", err)
	}
	fmt.Println("[PGDL]Dropping Region table...")
	_, err = dbInfo.DB.NewDropTable().Model((*pgTpch.Region)(nil)).IfExists().Cascade().Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop Regions table. Error:", err)
	}
}

func SendDropTablesRedirect(dbInfo DBInfo) {
	fmt.Println("[PGDL]Dropping LineItem table...")
	tables := []string{"lineitems", "orders", "customers", "partsupps", "suppliers", "parts", "nations", "regions"}
	err := pgTpch.SendProto(pgTpch.PB_DROP_TABLE, &proto.DropTable{Table: tables}, dbInfo.conn)
	if err != nil {
		fmt.Println("[PGDL]Failed to drop tables", tables, "Error:", err)
	}
	_, _, err = pgTpch.ReceiveProto(dbInfo.conn)
	if err != nil {
		fmt.Println("[PGDL]Failed to receive reply from dropping tables. Error:", err)
	}
}

func SendCreateTables(dbInfo DBInfo, tables *pgTpch.SQLTables) {
	res, err := dbInfo.DB.NewCreateTable().Model((*pgTpch.Region)(nil)).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE REGION. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE REGION: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.Nation)(nil)).ForeignKey(`("n_regionkey") REFERENCES "regions" ("r_regionkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE NATION. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE NATION: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.Part)(nil)).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE PART. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE PART: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.Supplier)(nil)).ForeignKey(`("s_nationkey") REFERENCES "nations" ("n_nationkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE SUPPLIER. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE SUPPLIER: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.PartSupp)(nil)).ForeignKey(`("ps_suppkey") REFERENCES "suppliers" ("s_suppkey") ON DELETE CASCADE`).
		ForeignKey(`("ps_partkey") REFERENCES "parts" ("p_partkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE PARTSUPP. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE PARTSUPP: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.Customer)(nil)).ForeignKey(`("c_nationkey") REFERENCES "nations" ("n_nationkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE CUSTOMER. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE CUSTOMER: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.Orders)(nil)).ForeignKey(`("o_custkey") REFERENCES "customers" ("c_custkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE ORDERS. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE ORDERS: %v\n", res)
	res, err = dbInfo.DB.NewCreateTable().Model((*pgTpch.LineItem)(nil)).ForeignKey(`("l_orderkey") REFERENCES "orders" ("o_orderkey") ON DELETE CASCADE`).
		ForeignKey(`("l_partkey") REFERENCES "parts" ("p_partkey") ON DELETE CASCADE`).ForeignKey(`("l_suppkey") REFERENCES "suppliers" ("s_suppkey") ON DELETE CASCADE`).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PGDL]Error on CREATE TABLE LINEITEM. Error:", err)
	}
	fmt.Printf("[PGDL]Reply from CREATE TABLE LINEITEM: %v\n", res)
	CreateTriggers(dbInfo)
	createTablesChan <- true
}

func SendCreateTablesRedirect(dbInfo DBInfo, tables *pgTpch.SQLTables) {
	tableIds := []int32{pgTpch.REGION, pgTpch.NATION, pgTpch.PART, pgTpch.SUPPLIER, pgTpch.PARTSUPP, pgTpch.CUSTOMER, pgTpch.ORDERS, pgTpch.LINEITEM}
	foreignKeys := make([]*proto.ForeignKey, len(tableIds))
	foreignKeys[0], foreignKeys[2] = &proto.ForeignKey{}, &proto.ForeignKey{}                                                                                                                              //region, part
	foreignKeys[1] = &proto.ForeignKey{ForeignKey: []string{`("n_regionkey") REFERENCES "regions" ("r_regionkey") ON DELETE CASCADE`}}                                                                     //nation
	foreignKeys[3] = &proto.ForeignKey{ForeignKey: []string{`("s_nationkey") REFERENCES "nations" ("n_nationkey") ON DELETE CASCADE`}}                                                                     //supplier
	foreignKeys[4] = &proto.ForeignKey{ForeignKey: []string{`("ps_suppkey") REFERENCES "suppliers" ("s_suppkey") ON DELETE CASCADE`, `("ps_partkey") REFERENCES "parts" ("p_partkey") ON DELETE CASCADE`}} //part supplier
	foreignKeys[5] = &proto.ForeignKey{ForeignKey: []string{`("c_nationkey") REFERENCES "nations" ("n_nationkey") ON DELETE CASCADE`}}                                                                     //customer
	foreignKeys[6] = &proto.ForeignKey{ForeignKey: []string{`("o_custkey") REFERENCES "customers" ("c_custkey") ON DELETE CASCADE`}}                                                                       //order
	foreignKeys[7] = &proto.ForeignKey{ForeignKey: []string{`("l_orderkey") REFERENCES "orders" ("o_orderkey") ON DELETE CASCADE`,
		`("l_partkey") REFERENCES "parts" ("p_partkey") ON DELETE CASCADE`, `("l_suppkey") REFERENCES "suppliers" ("s_suppkey") ON DELETE CASCADE`}} //lineitem
	err := pgTpch.SendProto(pgTpch.PB_CREATE_TABLE, &proto.CreateTable{TableId: tableIds, ForeignKeys: foreignKeys}, dbInfo.conn)
	if err != nil {
		fmt.Println("[PGDL]Failed to create tables. Error:", err)
	}
	_, _, err = pgTpch.ReceiveProto(dbInfo.conn)
	if err != nil {
		fmt.Println("[PGDL]Failed to receive reply from creating tables. Error:", err)
	}
}

func SendInserts(dbInfo DBInfo, procChan chan int, completeChan chan bool, tables *pgTpch.SQLTables) {
	nTables := len(tpch.TableNames)
	<-createTablesChan
	for i := 0; i < nTables; i++ {
		tableN := <-procChan
		if tableN == pgTpch.LINEITEM {
			continue //Skip; Do this one later after we get Parts and PartSupps
		}
		if tableN == pgTpch.PARTSUPP {
			continue //Skip; Do this one later after we get Parts
		}
		fmt.Println("[PGDL]Sending inserts for", getTableName(tableN))
		sendInsertHelper(dbInfo, tableN, tables)
	}
	fmt.Println("[PGDL]Sending inserts for", getTableName(pgTpch.PARTSUPP))
	sendInsertHelper(dbInfo, pgTpch.PARTSUPP, tables)
	fmt.Println("[PGDL]Sending inserts for", getTableName(pgTpch.LINEITEM))
	sendInsertHelper(dbInfo, pgTpch.LINEITEM, tables)
	fmt.Println("[PGDL]Finished all inserts.")
	testTable(dbInfo)
	completeChan <- true
}

func SendInsertsRedirect(dbInfo DBInfo, procChan chan int, completeChan chan bool, tables *pgTpch.SQLTables) {
	nTables := len(tpch.TableNames)
	<-createTablesChan
	for i := 0; i < nTables; i++ {
		tableN := <-procChan
		if tableN == pgTpch.LINEITEM {
			continue //Skip; Do this one later after we get Parts and PartSupps
		}
		if tableN == pgTpch.PARTSUPP {
			continue //Skip; Do this one later after we get Parts
		}
		fmt.Println("[PGDL]Sending inserts for", getTableName(tableN))
		sendInsertHelperRedirect(dbInfo, tableN, tables)
	}
	fmt.Println("[PGDL]Sending inserts for", getTableName(pgTpch.PARTSUPP))
	sendInsertHelperRedirect(dbInfo, pgTpch.PARTSUPP, tables)
	fmt.Println("[PGDL]Sending inserts for", getTableName(pgTpch.LINEITEM))
	sendInsertHelperRedirect(dbInfo, pgTpch.LINEITEM, tables)
	fmt.Println("[PGDL]Finished all inserts.")
	completeChan <- true
}

func getTableName(tableN int) string {
	switch tableN {
	case pgTpch.CUSTOMER:
		return "customers"
	case pgTpch.LINEITEM:
		return "lineitems"
	case pgTpch.NATION:
		return "nations"
	case pgTpch.ORDERS:
		return "orders"
	case pgTpch.PART:
		return "parts"
	case pgTpch.PARTSUPP:
		return "partsupps"
	case pgTpch.SUPPLIER:
		return "suppliers"
	case pgTpch.REGION:
		return "regions"
	}
	return "oups."
}

func sendInsertHelper(dbInfo DBInfo, tableN int, tables *pgTpch.SQLTables) {
	var res sql.Result
	var err error
	switch tableN {
	case pgTpch.CUSTOMER:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Customers).Exec(dbInfo.Ctx)
	case pgTpch.LINEITEM:
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
	case pgTpch.NATION:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Nations).Exec(dbInfo.Ctx)
	case pgTpch.ORDERS:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Orders).Exec(dbInfo.Ctx)
	case pgTpch.PART:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Parts).Exec(dbInfo.Ctx)
	case pgTpch.REGION:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Regions).Exec(dbInfo.Ctx)
		fmt.Printf("%v\n", tables.Regions)
	case pgTpch.PARTSUPP:
		res, err = dbInfo.DB.NewInsert().Model(&tables.PartSupps).Exec(dbInfo.Ctx)
	case pgTpch.SUPPLIER:
		res, err = dbInfo.DB.NewInsert().Model(&tables.Suppliers).Exec(dbInfo.Ctx)
	}
	if err != nil {
		fmt.Printf("[PGDL]Error on insert for table %s. Error: %v\n", getTableName(tableN), err)
	}
	fmt.Printf("[PGDL]Result for insert for table %s: %+v\n", getTableName(tableN), res)
}

func sendInsertHelperRedirect(dbInfo DBInfo, tableN int, tables *pgTpch.SQLTables) {
	var err error
	bulkInsert := &proto.BulkInsert{}
	switch tableN {
	case pgTpch.CUSTOMER:
		bulkInsert.Customers = make([]*proto.InsertCustomer, len(tables.Customers))
		for i, c := range tables.Customers {
			bulkInsert.Customers[i] = c.ToProto()
		}
	case pgTpch.LINEITEM:
		bulkInsert.LineItems = make([]*proto.InsertLineItem, len(tables.LineItems))
		for i, l := range tables.LineItems {
			bulkInsert.LineItems[i] = l.ToProto()
		}
	case pgTpch.NATION:
		bulkInsert.Nations = make([]*proto.InsertNation, len(tables.Nations))
		for i, n := range tables.Nations {
			bulkInsert.Nations[i] = n.ToProto()
		}
	case pgTpch.ORDERS:
		bulkInsert.Orders = make([]*proto.InsertOrder, len(tables.Orders))
		for i, o := range tables.Orders {
			bulkInsert.Orders[i] = o.ToProto()

		}
	case pgTpch.PART:
		bulkInsert.Parts = make([]*proto.InsertPart, len(tables.Parts))
		for i, p := range tables.Parts {
			bulkInsert.Parts[i] = p.ToProto()
		}
	case pgTpch.PARTSUPP:
		bulkInsert.PartSupps = make([]*proto.InsertPartSupp, len(tables.PartSupps))
		for i, ps := range tables.PartSupps {
			bulkInsert.PartSupps[i] = ps.ToProto()
		}
	case pgTpch.SUPPLIER:
		bulkInsert.Suppliers = make([]*proto.InsertSupplier, len(tables.Suppliers))
		for i, s := range tables.Suppliers {
			bulkInsert.Suppliers[i] = s.ToProto()
		}
	case pgTpch.REGION:
		bulkInsert.Regions = make([]*proto.InsertRegion, len(tables.Regions))
		for i, r := range tables.Regions {
			bulkInsert.Regions[i] = r.ToProto()
		}
	}
	err = pgTpch.SendProto(pgTpch.PB_BULK_INSERT, bulkInsert, dbInfo.conn)
	if err != nil {
		fmt.Printf("[PGDL]Error on insert for table %s. Error: %v\n", getTableName(tableN), err)
	}
	_, _, err = pgTpch.ReceiveProto(dbInfo.conn)
	if err != nil {
		fmt.Printf("[PGDL]Error on receive for insert of table %s. Error: %v\n", getTableName(tableN), err)
	}
}

func testTable(dbInfo DBInfo) {
	var result []pgTpch.Region
	err := dbInfo.DB.NewSelect().Model((*pgTpch.Region)(nil)).Column("*").Scan(dbInfo.Ctx, &result)
	if err != nil {
		fmt.Println("[PGDL]Error on the test table on regions:", err)
	}
	fmt.Println("[PGDL]Test table:", result)
}

// Equivalent of readUpdsByOrder() for PotionDB's tpch client.
func ReadUpdates(tpchData *tpch.TpchData, sqlTables *pgTpch.SQLTables, updateChan chan UpdateData) {
	start := time.Now().UnixNano()
	updPartsRead := [][]int8{tpchData.ToRead[tpch.ORDERS], tpchData.ToRead[tpch.LINEITEM]}
	orderUpds, lineItemUpds, deleteKeys, lineItemSizes, itemSizesPerOrder := tpch.ReadUpdatesPerOrder(tpch.UpdCompleteFilename[:], tpch.UpdEntries[:], UpdParts[:], updPartsRead, START_UPD_FILE, FINISH_UPD_FILE)
	timeTaken := (time.Now().UnixNano() - start) / int64(time.Millisecond)
	fmt.Printf("[PGDL]Time taken to read updates: %d ms\n", timeTaken)
	<-tablesReadyChan //Wait for Region, Nation and Customer's tables to be ready
	updateChan <- splitUpdatesPerRoutineAndRegion(sqlTables, orderUpds, lineItemUpds, deleteKeys, lineItemSizes, itemSizesPerOrder)
	//updateChan <- UpdateData{OrderUpds: orderUpds, LineItemUpds: lineItemUpds, DeleteKeys: deleteKeys, LineItemSizes: lineItemSizes, ItemSizesPerOrder: itemSizesPerOrder}
}

// Prepares updates to be used later by the clients. Similar logic used for PotionDB's TPCH client.
func splitUpdatesPerRoutineAndRegion(sqlTables *pgTpch.SQLTables, ordersUpds, lineItemUpds [][]string, deleteKeys []string, lineItemSizes, itemSizesPerOrder []int) (updData UpdateData) {
	fmt.Printf("[PGDL]Sizes of initial update data: orders %d, items %d, deletes %d, sizes %d\n", len(ordersUpds), len(lineItemUpds), len(deleteKeys), len(itemSizesPerOrder))
	nRegions, routines, offset := 5, int(N_CLIENTS), int32(0) //offset: correction of orderId -> location of order.
	//First, group by regions
	ordersPerRegion, itemsPerRegion, deletesPerRegion, lineSizesPerRegion := make([][][]string, nRegions), make([][][]string, nRegions), make([][]int32, nRegions), make([][]int, nRegions)
	for i := 0; i < nRegions; i++ {
		ordersPerRegion[i], itemsPerRegion[i] = make([][]string, 0, len(ordersUpds)/(nRegions-1)), make([][]string, 0, len(lineItemUpds)/(nRegions-1))
		deletesPerRegion[i], lineSizesPerRegion[i] = make([]int32, 0, len(deleteKeys)/(nRegions-1)), make([]int, 0, len(itemSizesPerOrder)/(nRegions-1))
	}
	if SF == 0.01 {
		offset = 0
	}

	currStartItem := 0
	for i, order := range ordersUpds {
		custKey, _ := strconv.ParseInt(order[tpch.O_CUSTKEY], 10, 64)
		region := sqlTables.CustkeyToRegionkey(custKey)
		nItems := itemSizesPerOrder[i]
		ordersPerRegion[region] = append(ordersPerRegion[region], order)
		itemsPerRegion[region] = append(itemsPerRegion[region], lineItemUpds[currStartItem:currStartItem+nItems]...)
		lineSizesPerRegion[region] = append(lineSizesPerRegion[region], nItems)
		currStartItem += nItems
		delKey, _ := strconv.ParseInt(deleteKeys[i], 10, 32)
		orderId := sqlTables.GetUpdateOrderIndex(int32(delKey))
		if orderId >= 1500000 {
			fmt.Printf("[PGDL]Error: orderId above 1500000!!! orderId %d, delKey %d\n", orderId, delKey)
			continue
		}
		delRegion := sqlTables.CustkeyToRegionkey(int64(sqlTables.Orders[orderId-offset].O_CUSTKEY))
		deletesPerRegion[delRegion] = append(deletesPerRegion[region], int32(delKey))
	}

	//Preparations for routine splitting
	ordersPerRoutine, remaining := len(ordersUpds)/routines, len(ordersUpds)%routines
	routineOrders, routineItems, routineDelete, routineLineSizes, regionForClient := make([][][]string, routines), make([][][]string, routines),
		make([][]int32, routines), make([][]int, routines), make([]int, routines)
	currRegion := 0
	//Make clients start in different regions. Will help for tests with very few clients
	currRegion, _ = strconv.Atoi(ID)
	currRegion = currRegion % nRegions
	currOrders, currItems, currDeletes, currSizes := ordersPerRegion[currRegion], itemsPerRegion[currRegion], deletesPerRegion[currRegion], lineSizesPerRegion[currRegion]
	orderStart, lineStart, orderFinish, lineFinish, j, leftForRoutine := 0, 0, 0, 0, 0, 0
	//leftForRoutine: used to store how many orders from the next region the client needs

	//Splitting by routines
	for i := 0; i < routines; i++ {
		regionForClient[i] = currRegion
		orderFinish += ordersPerRoutine
		if i < remaining { //Extra order
			orderFinish++
		}
		if orderFinish > len(currOrders) { //Not enough orders of region left. This client will have also orders from the next region.
			leftForRoutine = orderFinish - len(currOrders)
			orderFinish = len(currOrders)
			if leftForRoutine > ordersPerRoutine && routines > 2 {
				//Majority region is the next one
				regionForClient[i] = (currRegion + 1) % nRegions
			}
		}
		for j = orderStart; j < orderFinish; j++ {
			lineFinish += currSizes[j]
		}
		//fmt.Printf("[CU]Finished adding lineitemsizes. LineStart %d, lineFinish %d\n", lineStart, lineFinish)
		routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = make([][]string, orderFinish-orderStart),
			make([][]string, lineFinish-lineStart), make([]int32, orderFinish-orderStart), make([]int, orderFinish-orderStart)
		copy(routineOrders[i], currOrders[orderStart:orderFinish])
		copy(routineItems[i], currItems[lineStart:lineFinish])
		copy(routineDelete[i], currDeletes[orderStart:orderFinish])
		copy(routineLineSizes[i], currSizes[orderStart:orderFinish])
		orderStart, lineStart = orderFinish, lineFinish

		if orderFinish == len(currOrders) {
			//Next region
			currRegion = (currRegion + 1) % nRegions
			currOrders, currItems, currDeletes, currSizes = ordersPerRegion[currRegion], itemsPerRegion[currRegion], deletesPerRegion[currRegion], lineSizesPerRegion[currRegion]
			orderStart, orderFinish, lineStart, lineFinish, j = 0, 0, 0, 0, 0
		}

		//With 1 or 2 clients, this may happen more than once (or with more than 5 regions)
		for leftForRoutine > 0 {
			orderFinish += leftForRoutine
			if orderFinish > len(currOrders) {
				leftForRoutine = orderFinish - len(currOrders)
				orderFinish = len(currOrders)
			} else {
				leftForRoutine = 0
			}
			for j = orderStart; j < orderFinish; j++ {
				lineFinish += currSizes[j]
			}
			routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = append(routineOrders[i], currOrders[orderStart:orderFinish]...),
				append(routineItems[i], currItems[lineStart:lineFinish]...), append(routineDelete[i], currDeletes[orderStart:orderFinish]...),
				append(routineLineSizes[i], currSizes[orderStart:orderFinish]...)

			orderStart, lineStart = orderFinish, lineFinish

			if orderFinish == len(currOrders) {
				//Next region
				currRegion = (currRegion + 1) % nRegions
				currOrders, currItems, currDeletes, currSizes = ordersPerRegion[currRegion], itemsPerRegion[currRegion], deletesPerRegion[currRegion], lineSizesPerRegion[currRegion]
				orderStart, orderFinish, lineStart, lineFinish, j = 0, 0, 0, 0, 0
			}
		}
	}
	return UpdateData{RoutineOrders: routineOrders, RoutineItems: routineItems, RoutineDeletes: routineDelete, RoutineLineSizes: routineLineSizes, RegionPerClient: regionForClient}
}
