/*
@author: sk
@date: 2024/8/25
*/
package main

import "os"

// REDO LOG 用于事务提交双写，写入 REDO LOG 就算事务完成，可能此时数据页还没有写入磁盘 主要防止乱序写效率问题，REDO LOG 顺序写效率较高
// 在数据库崩溃重启时使用 REDO LOG 对提交还没有写入磁盘的事务进行数据恢复
// REDO LOG 也可以使用缓存但是需要在提交事件时必须写入磁盘，保证事务的提交
// REDO LOG 记录是增量数据，可以用于数据库间数据的同步(MySql是使用其抽象BinLog实现的主要是因为底层存储引擎的复杂性)
// UNDO LOG 在提交事务前所有操作都记录 事务未提交可能相关数据页已经写入磁盘了 UNDO LOG 在数据库崩溃重启时使用 UNDO LOG 对执行了一半没有提交的事务进行回滚
// 事务开始时产生 UNDO LOG 事务提交时删除 UNDO LOG 并双写 REDO LOG
// UNDO LOG 还可以用于实现 MVCC

// StartTx CommitTx RollbackTx CheckPoint
// 恢复时从最后一个CheckPoint向前 REDO ，注意要辨别页是否已经写入(LSN)，其中涉及到的事务且没有提交的倒序 UNDO

// UNDO LOG 倒着恢复直到恢复到事务开启
// 实现相关指令 不使用事务的话默认修改操作立即写磁盘
// BEGIN COMMIT ROLLBACK 暂时只实现 UNDO LOG 的功能

type UndoRecord struct {
}

type TransactionManager struct { // 简单实现只实现 UNDO LOG 没有支持多线程，也不需要事务id
	UndoLog *os.File // 事务中有值，否则为 nil
}

func (t *TransactionManager) Begin() {
	// 创建 undo.log 文件
}

func (t *TransactionManager) Commit() {
	// 必须在事务中才能提交即 UndoLog 文件必须存在  提交事务后不再需要回滚  删除 UndoLog 文件
}

func (t *TransactionManager) Rollback() {
	// 必须在事务中才能回滚即 UndoLog 文件必须存在  按 UndoLog 文件内容倒序回滚
}

func (t *TransactionManager) AddUndoRecord(record *UndoRecord) {
	if t.UndoLog == nil { // 不在事务中直接抛弃
		return
	}
	// 在事务中写入 UndoLog
}

// MVCC 实现  MySql 协议
