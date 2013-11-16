namespace java ly.stealth.thrift

include "Tag.thrift"

struct Meta {

	1: optional string id,
	2: optional list<Tag.Tag> tags,
	3: optional string datum
}
