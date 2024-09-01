/*
@author: sk
@date: 2024/9/1
*/
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
)

const (
	FeatLongPassword              = 1 << iota // new more secure passwords
	FeatFoundRows                             // Found instead of affected rows
	FeatLongFlag                              // Get all column flags
	FeatConnectWithDb                         // One can specify db on connect
	FeatNoSchema                              // Don't allow database.table.column
	FeatCompress                              // Can use compression protocol
	FeatOdbc                                  // Odbc client
	FeatLocalFiles                            // Can use LOAD DATA LOCAL
	FeatIgnoreSpace                           // Ignore spaces before '('
	FeatProtocol41                            // New 4.1 protocol
	FeatInteractive                           // This is an interactive client
	FeatSsl                                   // Switch to SSL after handshake
	FeatIgnoreSigpipe                         // IGNORE sigpipes
	FeatTransactions                          // Client knows about transactions
	FeatReserved                              // Old flag for 4.1 protocol
	FeatSecureConn                            // New 4.1 authentication
	FeatMultiStatements                       // Enable/disable multi-stmt support
	FeatMultiResults                          // Enable/disable multi-results
	FeatPsMultiResults                        // Enable/disable multiple resultsets for COM_STMT_EXECUTE
	FeatPluginAuth                            // Supports authentication plugins
	FeatConnectAttrs                          // Sends connection attributes in Protocol::HandshakeResponse41
	FeatPluginAuthData                        // Length of auth response data in Protocol::HandshakeResponse41 is a length-encoded integer
	FeatCanHandleExpiredPasswords             // Enable/disable expired passwords
	FeatSessionTrack                          // Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet.
	FeatDeprecateEof                          // Expects an OK (instead of EOF) after the resultset rows of a Text Resultset
)

const (
	// 最大包大小  16M
	MaxPackageSize = 16*1024*1024 - 1
)

const (
	CmdQuery = 0x03
)

const (
	AuthPlugin = "caching_sha2_password"
)

const (
	ColumnLong    = 0x03
	ColumnVarChar = 0xFD
	ColumnDouble  = 0x05
)

type Package struct {
	Len   uint32 // 3byte
	Num   uint8  // 1byte
	Data  []byte // Len byte
	Index int    // Data 读取下标
}

func (p *Package) Read(bs []byte) (n int, err error) {
	count := copy(bs, p.Data[p.Index:])
	p.Index += count
	return count, nil
}

func (p *Package) Reset() {
	p.Index = 0
}

func ReadPackage(reader io.Reader) *Package {
	len0 := ReadU24(reader)
	num := ReadU8(reader)
	data := ReadBytes(reader, len0)
	return &Package{
		Len:   len0,
		Num:   num,
		Data:  data,
		Index: 0,
	}
}

func WritePackage(writer io.Writer, pkg *Package) {
	WriteU24(writer, pkg.Len)
	WriteU8(writer, pkg.Num)
	WriteBytes(writer, pkg.Data)
}

func WriteU24(writer io.Writer, val uint32) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, val)
	_, err := writer.Write(bs[:3])
	HandleErr(err)
}

func ReadBytes(reader io.Reader, len0 uint32) []byte {
	bs := make([]byte, len0)
	_, err := reader.Read(bs)
	HandleErr(err)
	return bs
}

func ReadU8(reader io.Reader) uint8 {
	bs := make([]byte, 1)
	_, err := reader.Read(bs)
	HandleErr(err)
	return bs[0]
}

func ReadNStr(reader io.Reader) string {
	l := ReadU8(reader) // 这里存储的长度可能是大于 1byte 的暂时没有处理
	bs := ReadBytes(reader, uint32(l))
	return string(bs)
}

func ReadU24(reader io.Reader) uint32 {
	bs := make([]byte, 4)
	_, err := reader.Read(bs[:3]) // 最高位空着
	HandleErr(err)
	return binary.LittleEndian.Uint32(bs)
}

func ReadCStr(reader io.Reader) string {
	bs := make([]byte, 1)
	res := make([]byte, 0)
	for {
		_, err := reader.Read(bs)
		HandleErr(err)
		if bs[0] == 0x00 { // c_str 以 0x00 结尾
			return string(res)
		} else {
			res = append(res, bs[0])
		}
	}
}

func ReadU32(reader io.Reader) uint32 {
	bs := make([]byte, 4)
	_, err := reader.Read(bs)
	HandleErr(err)
	return binary.LittleEndian.Uint32(bs)
}

func ReadFull(reader io.Reader, bs []byte) {
	_, err := io.ReadFull(reader, bs)
	HandleErr(err)
}

func ReadU16(reader io.Reader) uint16 {
	bs := make([]byte, 2)
	_, err := reader.Read(bs)
	HandleErr(err)
	return binary.LittleEndian.Uint16(bs)
}

func WriteU32(writer io.Writer, val uint32) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, val)
	_, err := writer.Write(bs)
	HandleErr(err)
}

func WriteU8(writer io.Writer, val uint8) {
	_, err := writer.Write([]byte{val})
	HandleErr(err)
}

func WriteBytes(writer io.Writer, bs []byte) {
	_, err := writer.Write(bs)
	HandleErr(err)
}

func WriteCStr(writer io.Writer, str string) {
	_, err := writer.Write([]byte(str))
	HandleErr(err)
	_, err = writer.Write([]byte{0x00})
	HandleErr(err)
}

type Driver struct {
	// 用户输入信息
	Addr   string
	User   string
	Passwd string
	DB     string
	// 服务信息
	DBVersion  string
	EncryptKey []byte
	Flags      uint32 // 高2位是 客服端特性  低 2 位是服务端特性
	Lang       uint8
	// 中间过程信息
	Num uint8 // 单个通讯过程需要不断累加
}

func (d *Driver) Connect() *DB {
	conn, err := net.Dial("tcp", d.Addr)
	HandleErr(err)
	d.HandleGreeting(conn)
	d.HandleLogin(conn)
	d.HandleLoginResp(conn)
	return &DB{
		Driver: d,
		Conn:   conn,
	}
}

func (d *Driver) HandleGreeting(conn net.Conn) {
	pkg := ReadPackage(conn)
	d.Num = pkg.Num
	ReadU8(pkg)
	d.DBVersion = ReadCStr(pkg)
	ReadU32(pkg)
	ReadFull(pkg, d.EncryptKey[:8]) // 获取部分盐值
	ReadBytes(pkg, 1)
	d.Flags = uint32(ReadU16(pkg)) // 低 2 位
	d.Lang = ReadU8(pkg)
	ReadU16(pkg)
	d.Flags = d.Flags | (uint32(ReadU16(pkg)) << 16)
	ReadBytes(pkg, 11)
	ReadFull(pkg, d.EncryptKey[8:]) // 获取剩余盐值
	ReadBytes(pkg, 1)
	plugin := ReadCStr(pkg)
	if plugin != AuthPlugin { // 暂时只支持这一种认证方法，这种认证兼容大部分 MySql
		panic(fmt.Sprintf("not supported plugin: %s", plugin))
	}
}

func (d *Driver) HandleLogin(conn net.Conn) {
	flags := uint32(FeatProtocol41 | FeatLongPassword | FeatLongFlag | FeatTransactions | FeatConnectWithDb |
		FeatSecureConn | FeatLocalFiles | FeatMultiStatements | FeatMultiResults | FeatPluginAuth)
	flags &= d.Flags | 0xFFFF0000 // 服务端特性原样保持，添加客服端特性
	passwd := encryptPasswd(d.Passwd, d.EncryptKey)
	l := 4 + 4 + 1 + 23 + len(d.User) + 1 + len(passwd) + 1 + len(d.DB) + 1 + len(AuthPlugin) + 1

	buff := &bytes.Buffer{}
	WriteU32(buff, flags)
	WriteU32(buff, MaxPackageSize)
	WriteU8(buff, d.Lang)
	WriteBytes(buff, make([]byte, 23))
	WriteCStr(buff, d.User)
	WriteNStr(buff, string(passwd))
	WriteCStr(buff, d.DB)
	WriteCStr(buff, AuthPlugin)
	d.Num++ // 按要求写回包并做好 Num 自增
	pkg := &Package{
		Len:  uint32(l),
		Num:  d.Num,
		Data: buff.Bytes(),
	}
	WritePackage(conn, pkg)
}

func WriteNStr(writer io.Writer, val string) {
	WriteU8(writer, uint8(len(val)))
	WriteBytes(writer, []byte(val))
}

func (d *Driver) HandleLoginResp(conn net.Conn) {
	pkg1 := ReadPackage(conn)
	pkg2 := ReadPackage(conn)
	ReadBytes(pkg1, 1)
	authState := ReadU8(pkg1)
	state := ReadU8(pkg2) // 校验状态码无误
	if authState != 0x03 || state != 0x00 {
		panic(fmt.Sprintf("auth state error: %d %d", authState, state))
	}
	d.Num = pkg2.Num
}

func encryptPasswd(passwd string, encryptKey []byte) []byte {
	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), encryptKey))
	crypt := sha256.New()
	crypt.Write([]byte(passwd))
	msg1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(msg1)
	msg1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(msg1Hash)
	crypt.Write(encryptKey)
	msg2 := crypt.Sum(nil)

	for i := 0; i < len(msg1); i++ {
		msg1[i] ^= msg2[i]
	}
	return msg1
}

func NewDriver(addr string, user string, passwd string, db string) *Driver {
	return &Driver{Addr: addr, User: user, Passwd: passwd, DB: db, EncryptKey: make([]byte, 20)}
}

type ResultColumn struct {
	DBName    string
	TableName string
	Name      string
	Charset   uint16 // 先不管编码
	Type      uint8
	Flags     uint16 // 该列有啥特性
}

type Result struct {
	Columns []*ResultColumn
	Data    [][]string
	Index   int
}

func (r *Result) Next() bool {
	r.Index++
	return r.Index < len(r.Data)
}

func (r *Result) GetData(i int) any {
	data := r.Data[r.Index][i]
	switch r.Columns[i].Type {
	case ColumnLong:
		res, err := strconv.ParseInt(data, 10, 64)
		HandleErr(err)
		return res
	case ColumnDouble:
		res, err := strconv.ParseFloat(data, 64)
		HandleErr(err)
		return res
	case ColumnVarChar:
		return data
	default:
		panic(fmt.Sprintf("unknown column type: %d", r.Columns[i].Type))
	}
}

type DB struct {
	Driver *Driver
	Conn   net.Conn
}

func (d *DB) Query(sql string, args ...any) *Result {
	d.Driver.Num = 0
	if len(args) > 0 {
		sql = fmt.Sprintf(sql, args...)
	}

	buff := &bytes.Buffer{}
	WriteU8(buff, CmdQuery)
	WriteBytes(buff, []byte(sql))
	pkg := &Package{
		Len:  uint32(1 + len(sql)),
		Num:  d.Driver.Num,
		Data: buff.Bytes(),
	}
	WritePackage(d.Conn, pkg)

	// 先获取列数目
	pkg = ReadPackage(d.Conn)
	columnCount := ReadU8(pkg)
	// 循环获取所有列信息
	columns := make([]*ResultColumn, 0)
	for i := 0; i < int(columnCount); i++ {
		pkg = ReadPackage(d.Conn)
		ReadNStr(pkg)
		dbName := ReadNStr(pkg)
		ReadNStr(pkg)
		tableName := ReadNStr(pkg)
		ReadNStr(pkg)
		name := ReadNStr(pkg)
		ReadU8(pkg)
		charset := ReadU16(pkg)
		ReadU32(pkg)
		typ := ReadU8(pkg)
		flags := ReadU16(pkg)
		columns = append(columns, &ResultColumn{
			DBName:    dbName,
			TableName: tableName,
			Name:      name,
			Charset:   charset,
			Type:      typ,
			Flags:     flags,
		})
	}
	// 校验确实结束了
	pkg = ReadPackage(d.Conn)
	code := ReadU8(pkg)
	if code != 0xFE {
		panic(fmt.Sprintf("code error: 0x%x", code))
	}
	// 获取具体数据
	data := make([][]string, 0)
	for {
		pkg = ReadPackage(d.Conn)
		code = ReadU8(pkg)
		if code == 0xFE { // EOF 标记
			break
		}
		pkg.Reset()
		row := make([]string, 0)
		for i := 0; i < int(columnCount); i++ {
			row = append(row, ReadNStr(pkg))
		}
		data = append(data, row)
	}
	return &Result{
		Columns: columns,
		Data:    data,
		Index:   -1,
	}
}
