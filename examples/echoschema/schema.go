package echoschema

import fomoxa "github.com/fomoxa/cyclone-go"

const (
	SchemaFingerprint uint64 = 0x9E3779B97F4A7C15
	EchoMessageID     uint32 = 0x0000_0001
	ReplyMessageID    uint32 = 0x0000_0002
)

var (
	echoPrefixes  = []uint64{0x2545F4914F6CDD1D, 0x7FEB352D3F0A1B95}
	replyPrefixes = []uint64{0xD1B54A32D192ED03}
)

func New() *fomoxa.Schema {
	schema, err := fomoxa.NewSchema(SchemaFingerprint, []fomoxa.Message{
		{ID: EchoMessageID, Fingerprint: echoPrefixes[len(echoPrefixes)-1], Prefixes: echoPrefixes},
		{ID: ReplyMessageID, Fingerprint: replyPrefixes[len(replyPrefixes)-1], Prefixes: replyPrefixes},
	})
	if err != nil {
		panic(err)
	}
	return schema
}
