/*
@author: sk
@date: 2024/8/6
*/
package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
)

func main() {
	//var arr []string
	//fmt.Println(arr == nil)
	//arr = make([]string, 0)
	//fmt.Println(arr == nil)
	/*
		select a,b from t1
		select * from t2 where a > b
		select a,b from t3 where a > c order by b
		select a,count(*) from t4 where b > 100 group by a
		update t2 set n = 22,a = 33 where a > 100 AND b = 100
		insert into t3(a,b,z,d) values(2,3,2,4)
		delete from t3 where a = 100

		CREATE TABLE t2(uid int,name text)
		DROP TABLE t2
		CREATE INDEX idx ON t2(a,b)
		EXPLAIN  select * from t2
	*/
	//scanner := NewScanner("select a,b from t3 where a > c order by b")
	//tokens := scanner.ScanTokens()
	//parser := NewParser(tokens)
	//node := parser.ParseTokens()
	//transformer := NewTransformer(node)
	//query := transformer.TransformQuery()
	//fmt.Println(query)

	//reader := bufio.NewReader(os.Stdin)
	//fmt.Println("> welcome")
	//for {
	//	fmt.Print("$ ")
	//	text, err := reader.ReadString('\n')
	//	HandleErr(err)
	//	fmt.Println("> query: " + strings.TrimSpace(text))
	//	fmt.Printf("(Rows %d)\n", 0)
	//}

	//temp, err := os.Open("/Users/bytedance/Documents/go/my_sql/temp")
	//HandleErr(err)
	//defer temp.Close()
	//seek, err := temp.Seek(5, 0)
	//fmt.Println(seek, err)
	//seek, err = temp.Seek(0, 2)
	//fmt.Println(seek, err)
	//
	//temp.WriteString("Hello World")
	//temp.Seek(0, 0)
	//bs := make([]byte, 1024)
	//temp.Read(bs)
	//fmt.Println(string(bs))
	//temp.WriteString("hello world")
	//all, err := io.ReadAll(temp)
	//HandleErr(err)
	//fmt.Println(string(all))
	//stat, err := temp.Stat()
	//HandleErr(err)
	//fmt.Println(stat)
	//TestStorage()
	//TestBig()
	//TestOperator()
	//TestCombination()
	//TestDefer()

	//TestCmd()
	TestDriver()
}

func TestDriver() {
	driver := NewDriver("127.0.0.1:3306", "root", "12345678", "test")
	db := driver.Connect()
	res := db.Query("select * from test.test_table")
	fmt.Println(res.Columns[0].Name, res.Columns[1].Name, res.Columns[2].Name)
	for res.Next() {
		fmt.Println(res.GetData(0), res.GetData(1), res.GetData(2))
	}
}

func TestCmd() {
	/*  暂时不支持起别名
	select id,height from users
	select id,height,test(test(id)) from users limit 2
	select * from users where id > 20 AND id < 30
	select id,name from users where id > 20 order by id desc
	select distinct id,name from users
	select id,name from users limit 10 offset 8
	select name,count(id) from users where id > 30 group by name  -- 这里 count 不支持 * 必须使用字段
	select users.id,users.name,stud.uid,stud.height from users join stud on users.id = stud.uid where stud.uid < 100  -- JOIN 使用字段必须指定表名

	update stud set name = 'mysql',extra = 'a db' where uid > 100
	insert into stud values(1,22,'hello','world'),(2,33,'my','sql')  -- 必须填写全字段，不支持默认值
	delete from stud where id = 1

	CREATE TABLE stud(uid int,height float,name varchar(32),extra text)
	CREATE INDEX stud_idx ON stud(height,name)

	begin commit rollback exit
	*/
	LoadCatalog()
	defer SaveCatalog()
	storage := NewStorage()
	defer storage.Close()
	txManager := NewTransactionManager(storage)
	storage.TransactionManager = txManager

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("> welcome")
	for {
		fmt.Print("$ ")
		text, err := reader.ReadString('\n')
		HandleErr(err)
		text = strings.TrimSpace(text)
		fmt.Println("> query: " + text)

		switch strings.ToUpper(text) { // 对于输入内容需要先过指令，不满足任何指令才进行sql解析执行
		case CmdBegin:
			txManager.Begin()
		case CmdCommit:
			txManager.Commit()
		case CmdRollback:
			txManager.Rollback()
		case CmdExit:
			fmt.Println("bye")
			return
		default:
			scanner := NewScanner(text)
			tokens := scanner.ScanTokens()
			parser := NewParser(tokens)
			node := parser.ParseTokens()
			transformer := NewTransformer(node, storage)
			operator := transformer.Transform()

			operator.Open()
			PrintTable(operator)
			operator.Close()
		}
	}
}

func TestDefer() {
	defer fmt.Println("exit ....") // Ctrl + C 并不会执行
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("> welcome")
	for {
		fmt.Print("$ ")
		text, err := reader.ReadString('\n')
		HandleErr(err)
		fmt.Println("> query: " + strings.TrimSpace(text))
		fmt.Printf("(Rows %d)\n", 0)
	}
}

func TestCombination() {
	LoadCatalog()
	defer SaveCatalog()
	storage := NewStorage()
	defer storage.Close()
	txManager := NewTransactionManager(storage)
	storage.TransactionManager = txManager

	/*  暂时不支持起别名
	select id,height from users
	select * from users where id > 20 AND id < 30
	select id,name from users where id > 20 order by id desc
	select distinct id,name from users
	select id,name from users limit 10 offset 8
	select name,count(id) from users where id > 30 group by name  -- 这里 count 不支持 * 必须使用字段
	select users.id,users.name,stud.uid,stud.height from users join stud on users.id = stud.uid where stud.uid < 100  -- JOIN 使用字段必须指定表名

	update stud set name = 'mysql',extra = 'a db' where uid > 100
	insert into stud values(1,22,'hello','world'),(2,33,'my','sql')  -- 必须填写全字段，不支持默认值
	delete from stud where id = 1

	CREATE TABLE stud(uid int,height float,name varchar(32),extra text)
	CREATE INDEX stud_idx ON stud(height,name)

	begin commit rollback exit
	*/
	// 对于输入内容需要先过指令，不满足任何指令才进行sql解析执行
	scanner := NewScanner("select users.id,users.name,stud.uid,stud.height from users join stud on users.id = stud.uid where stud.uid < 100")
	tokens := scanner.ScanTokens()
	parser := NewParser(tokens)
	node := parser.ParseTokens()
	transformer := NewTransformer(node, storage)
	operator := transformer.Transform()

	operator.Open()
	defer operator.Close()
	PrintTable(operator)
}

func TestOperator() {
	storage := NewStorage()
	//for i := 0; i < 100; i++ {
	//	storage.InsertData("users", []any{int64(i), float64(2233), "tom", "helloAAA"})
	//}
	//left := NewTableScanOperator(storage, "users")
	//right := NewTableScanOperator(storage, "users")
	//operator := NewJoinOperator(left, right, &ExprNode{
	//	Left: &IDNode{
	//		Value: "users.id",
	//	},
	//	Right: &ImmNode{
	//		Value: "20",
	//	},
	//	Operator: &IDNode{
	//		Value: EQ,
	//	},
	//})

	//operator := NewFilterOperator(temp, &ExprNode{
	//	Left: &IDNode{
	//		Value: "users.id",
	//	},
	//	Right: &ImmNode{
	//		Value: "50",
	//	},
	//	Operator: &IDNode{
	//		Value: GE,
	//	},
	//})
	//operator := NewGroupOperator(temp, []string{"users.name"}, []*FuncNode{{
	//	FuncName: "max",
	//	Params: []INode{&IDNode{
	//		Value: "users.id",
	//	}},
	//}})
	//temp := NewTableScanOperator(storage, "users")
	//operator := NewSortOperator(temp, []*OrderNode{{
	//	Field: &IDNode{Value: "users.id"},
	//	Desc:  true,
	//}})
	temp := NewTableScanOperator(storage, "users")
	operator := NewLimitOperator(temp, 10, 10)
	operator.Open()
	fmt.Println(operator.GetColumns())
	for {
		res := operator.Next()
		if res != nil {
			fmt.Println(res)
		} else {
			break
		}
	}
	operator.Close()
	storage.Close()
}

func TestBig() {
	fmt.Println(ColumnCompare("aba", "aea", &Column{Type: TypStr}))
}

func TestStorage() {
	storage := NewStorage()
	//storage.InsertData("users", []any{int64(1122), 22.33, "tom", "hello world HA HA HA"})
	//storage.DeleteData("users", 0)
	//storage.UpdateData("users", 0, []any{int64(5566), 22.33, "tom", "hello world HA HA HA"})
	//fmt.Println(storage.SelectData("users", 0))
	//fmt.Println(storage.SelectIndex("users_index", []any{int64(5566), 22.33, "tom"}))
	arr := make([]int, 2000)
	for i := 0; i < len(arr); i++ {
		arr[i] = i
	}
	rand.Shuffle(2000, func(i, j int) {
		arr[i], arr[j] = arr[j], arr[i]
	})
	m := make(map[int]int)
	for _, item := range arr {
		m[item] = m[item]
	}
	fmt.Println(len(arr), len(m))
	for i := 0; i < 2000; i++ {
		//storage.InsertData("users", []any{int64(arr[i]), 22.33, "tim", "hello world HA HA HA"})
		//storage.InsertData("users", []any{int64(i), float64(2233), "tom", "helloAAA"})
	}
	for i := 0; i < 2000; i++ {
		//offset := storage.SelectIndex("users_index", []any{int64(i), float64(2233), "tom"})
		//fmt.Println(storage.SelectData("users", offset))
		offset := storage.SelectIndex("users_index", []any{int64(i), float64(2233), "tom"})
		fmt.Println(storage.SelectData("users", offset))
		//storage.DeleteData("users", offset)
		//storage.UpdateData("users", offset, []any{int64(i), float64(2233), "tom", "helloAAA"})
	}
	//for i := 0; i < 1000; i++ {
	//	//if i+2233 == 2280 {
	//	//	//fmt.Println("OK")
	//	//	break
	//	//}
	//	//offset := storage.SelectIndex("users_index", []any{int64(i + 2233), 22.33, "tom"})
	//	//storage.UpdateData("users", offset, []any{int64(i + 2233), 22.33, "tom", "NEW CONTENT"})
	//	//fmt.Println(storage.SelectData("users", offset))
	//	//storage.InsertData("users", []any{int64(i + 1), 22.33, "tom", "hello world HA HA HA"})
	//}
	storage.Close()
}
