//go:build vtprotobuf
// +build vtprotobuf
// Code generated by protoc-gen-go-vtproto. DO NOT EDIT.
// source: envoy/admin/v2alpha/memory.proto

package v2alpha

import (
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

func (m *Memory) MarshalVTStrict() (dAtA []byte, err error) {
	if m == nil {
		return nil, nil
	}
	size := m.SizeVT()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBufferVTStrict(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Memory) MarshalToVTStrict(dAtA []byte) (int, error) {
	size := m.SizeVT()
	return m.MarshalToSizedBufferVTStrict(dAtA[:size])
}

func (m *Memory) MarshalToSizedBufferVTStrict(dAtA []byte) (int, error) {
	if m == nil {
		return 0, nil
	}
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.unknownFields != nil {
		i -= len(m.unknownFields)
		copy(dAtA[i:], m.unknownFields)
	}
	if m.TotalPhysicalBytes != 0 {
		i = encodeVarint(dAtA, i, uint64(m.TotalPhysicalBytes))
		i--
		dAtA[i] = 0x30
	}
	if m.TotalThreadCache != 0 {
		i = encodeVarint(dAtA, i, uint64(m.TotalThreadCache))
		i--
		dAtA[i] = 0x28
	}
	if m.PageheapFree != 0 {
		i = encodeVarint(dAtA, i, uint64(m.PageheapFree))
		i--
		dAtA[i] = 0x20
	}
	if m.PageheapUnmapped != 0 {
		i = encodeVarint(dAtA, i, uint64(m.PageheapUnmapped))
		i--
		dAtA[i] = 0x18
	}
	if m.HeapSize != 0 {
		i = encodeVarint(dAtA, i, uint64(m.HeapSize))
		i--
		dAtA[i] = 0x10
	}
	if m.Allocated != 0 {
		i = encodeVarint(dAtA, i, uint64(m.Allocated))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *Memory) SizeVT() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Allocated != 0 {
		n += 1 + sov(uint64(m.Allocated))
	}
	if m.HeapSize != 0 {
		n += 1 + sov(uint64(m.HeapSize))
	}
	if m.PageheapUnmapped != 0 {
		n += 1 + sov(uint64(m.PageheapUnmapped))
	}
	if m.PageheapFree != 0 {
		n += 1 + sov(uint64(m.PageheapFree))
	}
	if m.TotalThreadCache != 0 {
		n += 1 + sov(uint64(m.TotalThreadCache))
	}
	if m.TotalPhysicalBytes != 0 {
		n += 1 + sov(uint64(m.TotalPhysicalBytes))
	}
	n += len(m.unknownFields)
	return n
}
