package pkg

import (
	"bytes"
	"encoding/gob"
)

type GOBMarshaller struct{}

func (m *GOBMarshaller) Marshal(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(data); err == nil {
		return buf.Bytes(), nil
	} else {
		return nil, err
	}
}

func (m *GOBMarshaller) Unmarshal(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(to)
}

func (m *GOBMarshaller) MarshalName() string { return "gob" }

func Encode(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(data); err == nil {
		return buf.Bytes(), nil
	} else {
		return nil, err
	}
}

func Decode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(to)
}
