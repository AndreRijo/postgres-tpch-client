package client

import "fmt"

func CreateOrderQuantityTrigger(dbInfo DBInfo) {
	function := "CREATE OR REPLACE FUNCTION update_sum_quantity() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$\n" +
		"BEGIN UPDATE orders SET o_sumquantity = o_sumquantity + NEW.l_quantity WHERE o_orderkey = NEW.l_orderkey; RETURN NEW; END; $$"
	result, err := dbInfo.DB.NewRaw(function).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PgQueryClient]Error on creating quantity function:", err)
	} else {
		fmt.Printf("[PgQueryClient]Success on creating quantity function: %+v\n", result)
	}
	trigger := "CREATE TRIGGER trigger_update_sum_quantity AFTER INSERT ON line_items FOR EACH ROW EXECUTE FUNCTION update_sum_quantity();"
	result, err = dbInfo.DB.NewRaw(trigger).Exec(dbInfo.Ctx)
	if err != nil {
		fmt.Println("[PgQueryClient]Error on creating quantity trigger:", err)
	} else {
		fmt.Printf("[PgQueryClient]Success on creating quantity trigger: %+v\n", result)
	}
}

func CreateTriggers(dbInfo DBInfo) {
	CreateOrderQuantityTrigger(dbInfo)
}

//CREATE OR REPLACE FUNCTION update_sum_quantity() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
//BEGIN UPDATE orders SET o_sumquantity = o_sumquantity + NEW.l_quantity WHERE o_orderkey = NEW.l_orderkey RETURN NEW END; $$

/*
CREATE OR REPLACE FUNCTION update_sum_quantity()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
  UPDATE orders
  SET o_sumquantity = o_sumquantity + NEW.l_quantity
  WHERE o_orderkey = NEW.l_orderkey
  RETURN NEW
END;
$$
*/

/*
CREATE TRIGGER trigger_update_sum_quantity
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_sum_quantity();
*/
