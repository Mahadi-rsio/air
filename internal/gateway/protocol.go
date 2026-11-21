package gateway

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	MaxStartupMessageBytes = 8192
	MaxPGPacketBytes       = 32 * 1024 * 1024
	CancelRequestCode      = 80877102
	SSLRequestCode         = 80877103
	GSSENCRequestCode      = 80877104
	PGProtocolVersion3     = 196608 // 3.0
)

func ReadCStr(buf []byte, off *int) string {
	start := *off
	for *off < len(buf) && buf[*off] != 0 {
		*off++
	}
	s := string(buf[start:*off])
	if *off < len(buf) && buf[*off] == 0 {
		*off++
	}
	return s
}

func appendCStr(buf []byte, s string) []byte {
	buf = append(buf, s...)
	return append(buf, 0)
}

func appendBE16(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

func appendBE32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

func BuildPGMessage(msgType byte, body []byte) []byte {
	out := make([]byte, 5+len(body))
	out[0] = msgType
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(body)))
	copy(out[5:], body)
	return out
}

func BuildErrorResponse(severity, sqlstate, msg string) []byte {
	var body []byte
	body = append(body, 'S')
	body = appendCStr(body, severity)
	body = append(body, 'C')
	body = appendCStr(body, sqlstate)
	body = append(body, 'M')
	body = appendCStr(body, msg)
	body = append(body, 0)
	return BuildPGMessage('E', body)
}

func BuildAuthOK() []byte {
	var body [4]byte
	return BuildPGMessage('R', body[:])
}

func BuildAuthCleartext() []byte {
	var body [4]byte
	binary.BigEndian.PutUint32(body[:], 3)
	return BuildPGMessage('R', body[:])
}

func BuildParamStatus(k, v string) []byte {
	var body []byte
	body = appendCStr(body, k)
	body = appendCStr(body, v)
	return BuildPGMessage('S', body)
}

func BuildBackendKeyData(pid, secret uint32) []byte {
	var body [8]byte
	binary.BigEndian.PutUint32(body[0:4], pid)
	binary.BigEndian.PutUint32(body[4:8], secret)
	return BuildPGMessage('K', body[:])
}

func BuildReadyForQuery(status byte) []byte {
	return BuildPGMessage('Z', []byte{status})
}

func BuildPasswordMessage(pw string) []byte {
	return BuildPGMessage('p', appendCStr(nil, pw))
}

func BuildSimpleQuery(sql string) []byte {
	return BuildPGMessage('Q', appendCStr(nil, sql))
}

func BuildSyncMessage() []byte {
	return BuildPGMessage('S', nil)
}

func BuildParseMessage(name, query string, oids []uint32) []byte {
	return BuildPGMessage('P', MakeParseBody(name, query, oids))
}

func MakeParseBody(stmtName, query string, oids []uint32) []byte {
	var body []byte
	body = appendCStr(body, stmtName)
	body = appendCStr(body, query)
	body = appendBE16(body, uint16(len(oids)))
	for _, oid := range oids {
		body = appendBE32(body, oid)
	}
	return body
}

func BuildCancelRequest(pid, secret uint32) []byte {
	var out [16]byte
	binary.BigEndian.PutUint32(out[0:4], 16)
	binary.BigEndian.PutUint32(out[4:8], CancelRequestCode)
	binary.BigEndian.PutUint32(out[8:12], pid)
	binary.BigEndian.PutUint32(out[12:16], secret)
	return out[:]
}

func BuildStartupMessage(params map[string]string) []byte {
	var kv []byte
	for k, v := range params {
		kv = appendCStr(kv, k)
		kv = appendCStr(kv, v)
	}
	kv = append(kv, 0)
	out := make([]byte, 8+len(kv))
	binary.BigEndian.PutUint32(out[0:4], uint32(8+len(kv)))
	binary.BigEndian.PutUint32(out[4:8], PGProtocolVersion3)
	copy(out[8:], kv)
	return out
}

// ReadPGPacket reads a typed PostgreSQL packet (1-byte type + 4-byte length + body),
// enforcing MaxPGPacketBytes.
func ReadPGPacket(r io.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	msgType := hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:5])
	if length < 4 || length > MaxPGPacketBytes {
		return 0, nil, fmt.Errorf("invalid PG packet length: %d", length)
	}
	bodyLen := int(length - 4)
	if bodyLen == 0 {
		return msgType, nil, nil
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}

// ParseStartupParams parses key/value pairs from a StartupMessage body (after version word).
func ParseStartupParams(body []byte) map[string]string {
	params := make(map[string]string)
	i := 0
	for i < len(body) {
		if body[i] == 0 {
			break
		}
		kEnd := i
		for kEnd < len(body) && body[kEnd] != 0 {
			kEnd++
		}
		if kEnd >= len(body) {
			break
		}
		key := string(body[i:kEnd])
		vStart := kEnd + 1
		vEnd := vStart
		for vEnd < len(body) && body[vEnd] != 0 {
			vEnd++
		}
		if vEnd > len(body) {
			break
		}
		params[key] = string(body[vStart:vEnd])
		i = vEnd + 1
	}
	return params
}

// RewriteParseBody rewrites a Parse ('P') body to use globalName, returning clientName, query, and parameter OIDs.
func RewriteParseBody(body []byte, globalName string) (rewritten []byte, clientName string, query string, oids []uint32) {
	off := 0
	clientName = ReadCStr(body, &off)
	query = ReadCStr(body, &off)
	if off+2 <= len(body) {
		n := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		oids = make([]uint32, 0, n)
		for i := 0; i < n && off+4 <= len(body); i++ {
			oids = append(oids, binary.BigEndian.Uint32(body[off:off+4]))
			off += 4
		}
	}
	return MakeParseBody(globalName, query, oids), clientName, query, oids
}

// RewriteBindBody rewrites a Bind ('B') body to reference the global statement name from stmts.
func RewriteBindBody(body []byte, stmts map[string]PreparedDef) []byte {
	off := 0
	portal := ReadCStr(body, &off)
	stmtName := ReadCStr(body, &off)
	tailOff := off

	target := stmtName
	if def, ok := stmts[stmtName]; ok {
		target = def.GlobalName
	}

	var out []byte
	out = appendCStr(out, portal)
	out = appendCStr(out, target)
	if tailOff < len(body) {
		out = append(out, body[tailOff:]...)
	}
	return out
}

// RewriteNamedOpBody rewrites a Describe ('D') or Close ('C') body when targeting a prepared statement ('S').
func RewriteNamedOpBody(body []byte, stmts map[string]PreparedDef, isClose bool) (rewritten []byte, erasedClientName string) {
	if len(body) == 0 || body[0] != 'S' {
		return body, ""
	}
	off := 1
	name := ReadCStr(body, &off)
	tailOff := off

	target := name
	if def, ok := stmts[name]; ok {
		target = def.GlobalName
		if isClose {
			erasedClientName = name
		}
	}

	var out []byte
	out = append(out, 'S')
	out = appendCStr(out, target)
	if tailOff < len(body) {
		out = append(out, body[tailOff:]...)
	}
	return out, erasedClientName
}
