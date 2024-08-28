/*
@author: sk
@date: 2024/8/18
*/
package main

const (
	BasePath = "data"
	ExtDat   = "dat" // 表数据存储文件 表名.dat 二进制 存储
	ExtIdx   = "idx" // 索引数据存储文件 索引名.idx b+tree 存储 是哪个表的索引通过 index_catalog表 获取
	ExtStr   = "str" // 不定长字符串存储 对应表名.str 索引不支持不定长文本
)

// 数据直接累加紧密存储 每条记录前一个 byte 标记该记录是否被删除  增加尾部增加  删除标记删除  更新直接更新
// 索引通过 b+tree 存储 key可能是多列 value 是偏移  还有软删标记 byte  增加直接增加  删除标记删除 更新先删除再增加
// 不定长字符串存储 长度(uint64)+内容
// 上面都是不管删除的 长时间使用要进行重建 根据有效的数据重建数据，索引，字符串常量集

const (
	CatalogTable = "table.catalog" // 其他表信息的元数据表
	CatalogIndex = "index.catalog" // 其他索引信息的元数据表
	UndoLog      = "undo.log"      // 采用尾添加的方式，读取时全部读取倒叙恢复
)

const (
	// 不允许插入 NULL 值
	TypInt   = 1 // int64
	TypFloat = 2 // float64
	TypStr   = 3 // string 定长的
	TypTxt   = 4 // string 不定长的
	TypBool  = 5 // 数据库中没有，条件判断中使用的
)

const (
	CmdBegin    = "BEGIN"
	CmdCommit   = "COMMIT"
	CmdRollback = "ROLLBACK"
	CmdExit     = "EXIT"
)
