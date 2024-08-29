/*
@author: sk
@date: 2024/8/15
*/
package main

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sort"
)

const (
	RecordIsDelete  = 1
	RecordNotDelete = 2
)

const (
	NodeData  = 1
	NodeIndex = 2
)

const (
	PageSize = 1024 * 4
)

type IndexHolder struct {
	Columns []*Column // 索引对应的列
}

func (h *IndexHolder) GetKeySize() int {
	return GetColumnSize(h.Columns)
}

func (h *IndexHolder) BatchCompare(vals1 []any, vals2 []any) int {
	return ColumnBatchCompare(vals1, vals2, h.Columns)
}

func (h *IndexHolder) BatchByte2Data(bs []byte) []any {
	return BatchByte2Data(bs, h.Columns, nil) // 用不到不定长文本
}

func (h *IndexHolder) BatchData2Byte(key []any) []byte {
	return BatchData2Byte(key, h.Columns, nil) // 用不到不定长文本
}

func NewIndexHolder(index string) *IndexHolder {
	temp := GetIndex(index)
	table := GetTable(temp.TableName)
	columns := PickColumn(temp.Columns, table.Columns)
	return &IndexHolder{Columns: columns}
}

type BTree struct { // 不定长文本需要再次检索不定长存储，索引不支持包含不定长文本
	File   *os.File
	Offset int64 // 再分配 offset 从哪里分配
	Root   *BTreeNode
	Index  *IndexHolder
}

func NewBTree(index string) *BTree {
	path0 := path.Join(BasePath, fmt.Sprintf("%s.%s", index, ExtIdx))
	file, err := os.OpenFile(path0, os.O_RDWR, 0666)
	holder := NewIndexHolder(index)
	root := &BTreeNode{Offset: 0, NodeType: NodeData, Index: holder}
	offset := int64(PageSize) // 若是文件不存在从 一页开始，第一页是根节点的
	if os.IsNotExist(err) {
		file, err = os.Create(path0)
		HandleErr(err)
	} else {
		HandleErr(err)
		root.Load(file)
		offset, err = file.Seek(0, 2)
		HandleErr(err)
	}
	return &BTree{File: file, Root: root, Index: holder, Offset: offset}
}

func (t *BTree) Close() {
	t.Sync() // 先同步节点状态 再关闭
	HandleErr(t.File.Close())
}

func (t *BTree) Sync() { // 所有节点都要写入 这里暂时不区分脏节点
	t.Root.Save(t.File)
}

func (t *BTree) AddData(key []any, val int64) {
	if len(key) != len(t.Index.Columns) {
		panic(fmt.Errorf("key len not match %d != %d", len(key), len(t.Index.Columns)))
	}
	// 添加前需要先查询，索引不允许添加重复值
	entry := t.GetEntry(key)
	if entry != nil { // 即使删除也不能重复，再加入有跟删除重复的就复用删除的
		if entry.Delete == RecordIsDelete {
			entry.Delete = RecordNotDelete
			entry.Data = val
			return // 可以直接复用，他也是排序的，直接结束
		} else {
			panic(fmt.Sprintf("key %v not unique", key))
		}
	}
	// 插入数据
	node := t.GetDataNode(t.Root, key)
	node.Entries = append(node.Entries, &BTreeEntry{Key: key, Delete: RecordNotDelete, Data: val})
	sort.Slice(node.Entries, func(i, j int) bool {
		return t.Index.BatchCompare(node.Entries[i].Key, node.Entries[j].Key) < 0
	})
	if t.Index.BatchCompare(node.Entries[0].Key, key) == 0 { // 特殊情况插入到第一个需要递归更新 key值
		t.UpdateKey(node, key)
	}
	// 判断是否要分裂
	if t.NeedSplit(node) {
		t.SplitNode(node)
	}
}

func (t *BTree) NextOffset() int64 {
	res := t.Offset
	t.Offset += PageSize
	return res
}

func (t *BTree) SplitNode(node *BTreeNode) {
	parent := node.Parent
	left := &BTreeNode{NodeType: node.NodeType, Index: t.Index}
	right := &BTreeNode{NodeType: node.NodeType, Index: t.Index}
	if parent == nil { // 根节点进行分裂
		parent = &BTreeNode{
			Offset:   0,         // 根节点必须存储在 0 号位置
			NodeType: NodeIndex, // 一旦分裂新的根节点就必定是索引节点了
			Index:    t.Index,
		}
		t.Root = parent              // 新的根节点
		left.Offset = t.NextOffset() // 根节点分裂需要创建两个数据页
		right.Offset = t.NextOffset()
	} else {
		// 非根节点先移除 node 节点，方便后面无脑添加  left right 节点
		entries := make([]*BTreeEntry, 0)
		for _, entry := range parent.Entries {
			if entry.Node != node {
				entries = append(entries, entry)
			}
		}
		parent.Entries = entries
		left.Offset = node.Offset     // 复用原来的数据页
		right.Offset = t.NextOffset() // 非根节点分裂只需要申请一个数据页
	}
	left.Parent = parent
	right.Parent = parent
	l := len(node.Entries) / 2
	// 这里是使用同一个 slice 需要进行复制，防止 append 后复用
	left.Entries = CloneSlice(node.Entries[:l])
	right.Entries = CloneSlice(node.Entries[l:])
	if node.NodeType == NodeIndex { // 索引节点的话需要更新子节点到父节点的反向引用
		for _, entry := range left.Entries {
			entry.LoadNode(t.File, t.Index, left) // 加载时需要更新上 父关联，但是不一定触发加载
			entry.Node.Parent = left
		}
		for _, entry := range right.Entries {
			entry.LoadNode(t.File, t.Index, right)
			entry.Node.Parent = right
		}
	} else if node.NodeType == NodeData { // 数据节点没有子节点，但是需要更新前后索引方便进行遍历
		node.LoadPreAndNext(t.File, parent)
		left.Pre = node.Pre
		left.PreOffset = node.PreOffset
		if node.Pre != nil {
			node.Pre.Next = left
			node.Pre.NextOffset = left.Offset
		}
		left.Next = right
		left.NextOffset = right.Offset
		right.Pre = left
		right.PreOffset = left.Offset
		right.Next = node.Next
		right.NextOffset = node.NextOffset
		if node.Next != nil {
			node.Next.Pre = right
			node.Next.PreOffset = right.Offset
		}
	} // 重新加入子节点并排序
	parent.Entries = append(parent.Entries, &BTreeEntry{
		Key:    left.Entries[0].Key, // 这里parent肯定是索引节点
		Offset: left.Offset,
		Node:   left,
	}, &BTreeEntry{
		Key:    right.Entries[0].Key, // key取整个元素中最小的
		Offset: right.Offset,
		Node:   right,
	})
	sort.Slice(parent.Entries, func(i, j int) bool {
		return t.Index.BatchCompare(parent.Entries[i].Key, parent.Entries[j].Key) < 0
	})
	if t.NeedSplit(parent) { // 可能会触发递归分裂
		t.SplitNode(parent)
	}
}

func (t *BTree) NeedSplit(node *BTreeNode) bool {
	switch node.NodeType { // 超出一页数据就需要分页 一页最多 256个数据
	case NodeData: // NodeType 1byte PreOffset 8byte NextOffset 8byte entry_count 1byte entry_list( key nbyte delete 1byte data 8byte )
		size := t.Index.GetKeySize()
		return 1+8+8+1+len(node.Entries)*(size+1+8) > PageSize
	case NodeIndex: // NodeType 1byte entry_count 1byte entry_list( key nbyte offset 8byte )
		size := t.Index.GetKeySize()
		return 1+1+len(node.Entries)*(size+8) > PageSize
	default:
		panic(fmt.Sprintf("node type not support: %v", node.NodeType))
	}
}

func (t *BTree) GetDataNode(node *BTreeNode, key []any) *BTreeNode {
	if node.NodeType == NodeData {
		return node
	}
	l, r := 0, len(node.Entries)-1
	for l < r { // 找最小的 大于等于的节点 若是小于所有节点返回第一个节点
		mid := (l + r + 1) / 2
		if t.Index.BatchCompare(key, node.Entries[mid].Key) < 0 {
			r = mid - 1
		} else {
			l = mid
		}
	}
	node.Entries[l].LoadNode(t.File, t.Index, node)
	return t.GetDataNode(node.Entries[l].Node, key)
}

func (t *BTree) GetEntry(key []any) *BTreeEntry {
	node := t.GetDataNode(t.Root, key)
	l, r := 0, len(node.Entries)-1
	for l <= r {
		mid := (l + r) / 2
		res := t.Index.BatchCompare(key, node.Entries[mid].Key)
		if res > 0 {
			l = mid + 1
		} else if res < 0 {
			r = mid - 1
		} else {
			return node.Entries[mid]
		}
	}
	return nil
}

func (t *BTree) UpdateKey(node *BTreeNode, key []any) {
	// 更新自己在父节点中的 key 值
	parent := node.Parent // 这里认为父节点都是存在的
	if parent == nil {
		return
	}
	for i, entry := range parent.Entries {
		if entry.Node == node {
			entry.Key = key
			if i == 0 { // 若更新的是父节点的第一个节点需要递归更新
				t.UpdateKey(parent, key)
			}
			break
		}
	}
}

func (t *BTree) DelData(key []any) {
	if entry := t.GetEntry(key); entry != nil {
		entry.Delete = RecordIsDelete
	} else {
		panic(fmt.Sprintf("key not exist: %v", key))
	}
}

func (t *BTree) GetData(key []any) int64 {
	entry := t.GetEntry(key)
	if entry == nil || entry.Delete == RecordIsDelete {
		panic(fmt.Sprintf("record not exists key = %v", key))
	}
	return entry.Data
}

func (t *BTree) GetFirstNode() *BTreeNode {
	node := t.Root
	for node.NodeType != NodeData {
		node.Entries[0].LoadNode(t.File, t.Index, node)
		node = node.Entries[0].Node
	}
	return node
}

func (t *BTree) GetNextNode(node *BTreeNode) *BTreeNode {
	if node.Next != nil { // 已经加载的话直接使用
		return node.Next
	}
	if node.NextOffset == 0 {
		return nil
	}
	// 这里缺失 parent 字段不能直接加入 BTree 仅临时使用
	res := &BTreeNode{Offset: node.NextOffset, Index: t.Index, NodeType: NodeData}
	res.Load(t.File)
	return res
}

type BTreeEntry struct {
	Key []any // 排序字段 组合排序
	// 索引
	Offset int64
	Node   *BTreeNode // 不一定立即加载
	// 数据
	Delete uint8
	Data   int64
}

func (e *BTreeEntry) LoadNode(file *os.File, index *IndexHolder, parent *BTreeNode) {
	if e.Node != nil { // 以内存为准
		return
	}
	e.Node = &BTreeNode{Offset: e.Offset, Index: index, Parent: parent}
	e.Node.Load(file)
}

type BTreeNode struct {
	Index  *IndexHolder
	Parent *BTreeNode
	Offset int64
	//Dirty  bool  先不使用脏标记，采用全部写入的方式
	// 需要写入文件的
	NodeType uint8
	Entries  []*BTreeEntry
	// 只有数据节点需要，链接前后进行遍历的
	Pre        *BTreeNode
	PreOffset  int64
	Next       *BTreeNode
	NextOffset int64
}

func (n *BTreeNode) Save(file *os.File) {
	// 先保存自己
	buff := &bytes.Buffer{}
	buff.WriteByte(n.NodeType)
	if n.NodeType == NodeData {
		buff.Write(Int64ToByte(n.PreOffset))
		buff.Write(Int64ToByte(n.NextOffset))
		buff.WriteByte(byte(len(n.Entries)))
		for _, entry := range n.Entries {
			buff.Write(n.Index.BatchData2Byte(entry.Key))
			buff.WriteByte(entry.Delete)
			buff.Write(Int64ToByte(entry.Data))
		}
	} else if n.NodeType == NodeIndex {
		buff.WriteByte(byte(len(n.Entries)))
		for _, entry := range n.Entries {
			buff.Write(n.Index.BatchData2Byte(entry.Key))
			buff.Write(Int64ToByte(entry.Offset))
		}
	}
	_, err := file.Seek(n.Offset, 0)
	HandleErr(err)
	bs := make([]byte, PageSize) // 必须补齐一页
	copy(bs, buff.Bytes())
	_, err = file.Write(bs)
	HandleErr(err)
	// 索引节点需要进行递归
	if n.NodeType == NodeIndex {
		for _, entry := range n.Entries {
			if entry.Node != nil { // 只有加载了才需要保存
				entry.Node.Save(file)
			}
		}
	}
}

func (n *BTreeNode) Load(file *os.File) {
	_, err := file.Seek(n.Offset, 0)
	HandleErr(err)
	bs := make([]byte, PageSize)
	_, err = file.Read(bs)
	HandleErr(err)
	n.NodeType = bs[0]
	if n.NodeType == NodeData {
		n.PreOffset = ByteToInt64(bs[1:9])
		n.NextOffset = ByteToInt64(bs[9:17])
		entryCount := bs[17]
		idx := 18
		size := n.Index.GetKeySize()
		for i := 0; i < int(entryCount); i++ {
			key := n.Index.BatchByte2Data(bs[idx : idx+size])
			n.Entries = append(n.Entries, &BTreeEntry{
				Key:    key,
				Delete: bs[idx+size],
				Data:   ByteToInt64(bs[idx+size+1 : idx+size+1+8]),
			})
			idx += size + 1 + 8
		}
	} else if n.NodeType == NodeIndex {
		entryCount := bs[1]
		idx := 2
		size := n.Index.GetKeySize()
		for i := 0; i < int(entryCount); i++ {
			key := n.Index.BatchByte2Data(bs[idx : idx+size])
			n.Entries = append(n.Entries, &BTreeEntry{
				Key:    key,
				Offset: ByteToInt64(bs[idx+size : idx+size+8]),
			})
			idx += size + 8
		}
	} else {
		panic(fmt.Sprintf("node type not support: %v", n.NodeType))
	}
}

func (n *BTreeNode) LoadPreAndNext(file *os.File, parent *BTreeNode) {
	if n.Pre == nil && n.PreOffset != 0 {
		n.Pre = &BTreeNode{Offset: n.PreOffset, Index: n.Index, Parent: parent}
		n.Pre.Load(file)
	}
}

type Storage struct {
	TableFiles         map[string]*os.File // 表名 -> 文件
	StringFiles        map[string]*os.File // 表名 -> 文件
	IndexTrees         map[string]*BTree   // 索引名称 -> BTree
	TransactionManager *TransactionManager
}

func NewStorage() *Storage {
	return &Storage{TableFiles: make(map[string]*os.File), StringFiles: make(map[string]*os.File),
		IndexTrees: make(map[string]*BTree)}
}

func (s *Storage) Close() {
	for _, file := range s.TableFiles {
		HandleErr(file.Close())
	}
	for _, file := range s.StringFiles {
		HandleErr(file.Close())
	}
	for _, tree := range s.IndexTrees {
		tree.Close()
	}
}

func (s *Storage) Sync(table string) {
	// 同步一个表
	if file, ok := s.TableFiles[table]; ok {
		HandleErr(file.Sync())
	}
	if file, ok := s.StringFiles[table]; ok {
		HandleErr(file.Sync())
	}
	idxes := ListIndexes(table)
	for _, idx := range idxes {
		if tree, ok := s.IndexTrees[idx.Name]; ok {
			tree.Sync()
		}
	}
}

func (s *Storage) OpenTable(table string) *os.File {
	if _, ok := s.TableFiles[table]; !ok {
		s.TableFiles[table] = OpenOrCreate(path.Join(BasePath, fmt.Sprintf("%s.%s", table, ExtDat)))
	}
	return s.TableFiles[table]
}

func (s *Storage) OpenString(table string) *os.File {
	if _, ok := s.StringFiles[table]; !ok {
		s.StringFiles[table] = OpenOrCreate(path.Join(BasePath, fmt.Sprintf("%s.%s", table, ExtStr)))
	}
	return s.StringFiles[table]
}

func (s *Storage) OpenIndex(index string) *BTree {
	if _, ok := s.IndexTrees[index]; !ok {
		s.IndexTrees[index] = NewBTree(index)
	}
	return s.IndexTrees[index]
}

func (s *Storage) WriteTxt(table string, txt string) int64 {
	file := s.OpenString(table)
	offset, err := file.Seek(0, 2)
	HandleErr(err)
	// 先写入长度再写入内容
	_, err = file.Write(Int64ToByte(int64(len(txt))))
	HandleErr(err)
	_, err = file.WriteString(txt)
	HandleErr(err)
	return offset
}

func (s *Storage) ReadTxt(table string, offset int64) string {
	file := s.OpenString(table)
	_, err := file.Seek(offset, 0)
	HandleErr(err)
	// 先读取长度
	bs := make([]byte, 8)
	_, err = file.Read(bs)
	HandleErr(err)
	l := ByteToInt64(bs)

	bs = make([]byte, l)
	_, err = file.Read(bs)
	HandleErr(err)
	return string(bs)
}

// 添加一行数据到末尾 data 全字段
func (s *Storage) InsertData(table string, data []any) {
	// 写入基础数据
	meta := GetTable(table)
	file := s.OpenTable(table)
	offset, err := file.Seek(0, 2)
	HandleErr(err)
	bs := BatchData2Byte(data, meta.Columns, func(value string) int64 {
		return s.WriteTxt(table, value)
	})
	bs = append([]byte{RecordNotDelete}, bs...) // 默认肯定是没有删除的
	_, err = file.Write(bs)
	HandleErr(err)
	// 写入索引
	indexes0 := ListIndexes(table)
	for _, index := range indexes0 {
		data0 := PickData(index.Columns, meta.Columns, data)
		btree := s.OpenIndex(index.Name)
		btree.AddData(data0, offset)
	}
	s.TransactionManager.AddUndoRecord(&UndoRecord{
		Type:   UndoInsert,
		Table:  table,
		Offset: offset,
	})
}

// 删除一行数据 offset 偏移
func (s *Storage) DeleteData(table string, offset int64) {
	// 删除索引，删除前需要先查询到对应的 key
	data := s.SelectData(table, offset)
	meta := GetTable(table)
	indexes0 := ListIndexes(table)
	for _, index := range indexes0 {
		data0 := PickData(index.Columns, meta.Columns, data)
		btree := s.OpenIndex(index.Name)
		btree.DelData(data0)
	}
	// 标记删除
	file := s.OpenTable(table)
	_, err := file.Seek(offset, 0)
	HandleErr(err)
	_, err = file.Write([]byte{RecordIsDelete})
	HandleErr(err)
	s.TransactionManager.AddUndoRecord(&UndoRecord{
		Type:  UndoDelete,
		Table: table,
		Data:  data,
	})
}

// 修改一行数据 offset 偏移 data 全字段，覆盖更新，主要方便索引更新
func (s *Storage) UpdateData(table string, offset int64, data []any) {
	s.DeleteData(table, offset)
	s.InsertData(table, data)
}

// 更具偏移获取数据
func (s *Storage) SelectData(table string, offset int64) []any {
	file := s.OpenTable(table)
	meta := GetTable(table)
	_, err := file.Seek(offset, 0)
	HandleErr(err)
	size := GetColumnSize(meta.Columns) + 1
	bs := make([]byte, size)
	_, err = file.Read(bs)
	HandleErr(err)
	if bs[0] != RecordNotDelete {
		panic(fmt.Sprintf("record %v is not exist", offset))
	} // 移除删除标记
	return BatchByte2Data(bs[1:], meta.Columns, func(offset int64) string {
		return s.ReadTxt(table, offset)
	})
}

// offset 第一次传 0 就行了 后面使用返回值
func (s *Storage) NextData(table string, offset int64) ([]any, int64, int64) {
	meta := GetTable(table)
	size := GetColumnSize(meta.Columns) + 1
	// 寻找记录
	file := s.OpenTable(table)
	maxOffset, err := file.Seek(0, 2)
	HandleErr(err)
	bs := make([]byte, size)
	for offset < maxOffset {
		_, err = file.Seek(offset, 0)
		HandleErr(err)
		_, err = file.Read(bs)
		HandleErr(err)
		if bs[0] == RecordNotDelete {
			return BatchByte2Data(bs[1:], meta.Columns, func(offset int64) string {
				return s.ReadTxt(table, offset)
			}), offset, offset + int64(size)
		}
		offset += int64(size)
	}
	return nil, 0, 0 // 没有了
}

func (s *Storage) SelectIndex(index string, keys []any) int64 {
	index0 := s.OpenIndex(index)
	return index0.GetData(keys)
}

// 第一次 node 传 nil idx 传 0
func (s *Storage) NextIndex(index string, node *BTreeNode, idx int) ([]any, *BTreeNode, int) {
	index0 := s.OpenIndex(index)
	if node == nil {
		node = index0.GetFirstNode()
	}
	for {
		if idx < len(node.Entries) {
			if node.Entries[idx].Delete == RecordNotDelete {
				return append(node.Entries[idx].Key, node.Entries[idx].Data), node, idx + 1
			}
			idx++
		} else {
			node = index0.GetNextNode(node)
			if node == nil { // 没了
				return nil, nil, 0
			}
			idx = 0
		}
	}
}
