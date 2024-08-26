/*
@author: sk
@date: 2024/8/25
*/
package main

// REDO LOG 用于事务提交双写，写入 REDO LOG 就算事务完成，可能此时数据页还没有写入磁盘 主要防止乱序写效率问题，REDO LOG 顺序写效率较高
// 在数据库崩溃重启时使用 REDO LOG 对提交还没有写入磁盘的事务进行数据恢复
// REDO LOG 也可以使用缓存但是需要在提交事件时必须写入磁盘，保证事务的提交
// UNDO LOG 在提交事务前所有操作都记录 事务未提交可能相关数据页已经写入磁盘了 UNDO LOG 在数据库崩溃重启时使用 UNDO LOG 对执行了一半没有提交的事务进行回滚
// 事务开始时产生 UNDO LOG 事务提交时删除 UNDO LOG 并双写 REDO LOG
