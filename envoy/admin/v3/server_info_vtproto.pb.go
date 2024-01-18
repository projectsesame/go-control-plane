//go:build vtprotobuf
// +build vtprotobuf
// Code generated by protoc-gen-go-vtproto. DO NOT EDIT.
// source: envoy/admin/v3/server_info.proto

package adminv3

import (
	durationpb "github.com/planetscale/vtprotobuf/types/known/durationpb"
	proto "google.golang.org/protobuf/proto"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

func (m *ServerInfo) MarshalVTStrict() (dAtA []byte, err error) {
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

func (m *ServerInfo) MarshalToVTStrict(dAtA []byte) (int, error) {
	size := m.SizeVT()
	return m.MarshalToSizedBufferVTStrict(dAtA[:size])
}

func (m *ServerInfo) MarshalToSizedBufferVTStrict(dAtA []byte) (int, error) {
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
	if m.Node != nil {
		if vtmsg, ok := interface{}(m.Node).(interface {
			MarshalToSizedBufferVTStrict([]byte) (int, error)
		}); ok {
			size, err := vtmsg.MarshalToSizedBufferVTStrict(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarint(dAtA, i, uint64(size))
		} else {
			encoded, err := proto.Marshal(m.Node)
			if err != nil {
				return 0, err
			}
			i -= len(encoded)
			copy(dAtA[i:], encoded)
			i = encodeVarint(dAtA, i, uint64(len(encoded)))
		}
		i--
		dAtA[i] = 0x3a
	}
	if m.CommandLineOptions != nil {
		size, err := m.CommandLineOptions.MarshalToSizedBufferVTStrict(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarint(dAtA, i, uint64(size))
		i--
		dAtA[i] = 0x32
	}
	if len(m.HotRestartVersion) > 0 {
		i -= len(m.HotRestartVersion)
		copy(dAtA[i:], m.HotRestartVersion)
		i = encodeVarint(dAtA, i, uint64(len(m.HotRestartVersion)))
		i--
		dAtA[i] = 0x2a
	}
	if m.UptimeAllEpochs != nil {
		size, err := (*durationpb.Duration)(m.UptimeAllEpochs).MarshalToSizedBufferVTStrict(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarint(dAtA, i, uint64(size))
		i--
		dAtA[i] = 0x22
	}
	if m.UptimeCurrentEpoch != nil {
		size, err := (*durationpb.Duration)(m.UptimeCurrentEpoch).MarshalToSizedBufferVTStrict(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarint(dAtA, i, uint64(size))
		i--
		dAtA[i] = 0x1a
	}
	if m.State != 0 {
		i = encodeVarint(dAtA, i, uint64(m.State))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Version) > 0 {
		i -= len(m.Version)
		copy(dAtA[i:], m.Version)
		i = encodeVarint(dAtA, i, uint64(len(m.Version)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *CommandLineOptions) MarshalVTStrict() (dAtA []byte, err error) {
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

func (m *CommandLineOptions) MarshalToVTStrict(dAtA []byte) (int, error) {
	size := m.SizeVT()
	return m.MarshalToSizedBufferVTStrict(dAtA[:size])
}

func (m *CommandLineOptions) MarshalToSizedBufferVTStrict(dAtA []byte) (int, error) {
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
	if len(m.StatsTag) > 0 {
		for iNdEx := len(m.StatsTag) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.StatsTag[iNdEx])
			copy(dAtA[i:], m.StatsTag[iNdEx])
			i = encodeVarint(dAtA, i, uint64(len(m.StatsTag[iNdEx])))
			i--
			dAtA[i] = 0x2
			i--
			dAtA[i] = 0xb2
		}
	}
	if m.EnableCoreDump {
		i--
		if m.EnableCoreDump {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x2
		i--
		dAtA[i] = 0xa8
	}
	if m.SocketMode != 0 {
		i = encodeVarint(dAtA, i, uint64(m.SocketMode))
		i--
		dAtA[i] = 0x2
		i--
		dAtA[i] = 0xa0
	}
	if len(m.SocketPath) > 0 {
		i -= len(m.SocketPath)
		copy(dAtA[i:], m.SocketPath)
		i = encodeVarint(dAtA, i, uint64(len(m.SocketPath)))
		i--
		dAtA[i] = 0x2
		i--
		dAtA[i] = 0x9a
	}
	if m.EnableFineGrainLogging {
		i--
		if m.EnableFineGrainLogging {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x2
		i--
		dAtA[i] = 0x90
	}
	if m.DrainStrategy != 0 {
		i = encodeVarint(dAtA, i, uint64(m.DrainStrategy))
		i--
		dAtA[i] = 0x2
		i--
		dAtA[i] = 0x88
	}
	if len(m.BaseIdPath) > 0 {
		i -= len(m.BaseIdPath)
		copy(dAtA[i:], m.BaseIdPath)
		i = encodeVarint(dAtA, i, uint64(len(m.BaseIdPath)))
		i--
		dAtA[i] = 0x2
		i--
		dAtA[i] = 0x82
	}
	if m.UseDynamicBaseId {
		i--
		if m.UseDynamicBaseId {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xf8
	}
	if m.IgnoreUnknownDynamicFields {
		i--
		if m.IgnoreUnknownDynamicFields {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xf0
	}
	if len(m.DisabledExtensions) > 0 {
		for iNdEx := len(m.DisabledExtensions) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.DisabledExtensions[iNdEx])
			copy(dAtA[i:], m.DisabledExtensions[iNdEx])
			i = encodeVarint(dAtA, i, uint64(len(m.DisabledExtensions[iNdEx])))
			i--
			dAtA[i] = 0x1
			i--
			dAtA[i] = 0xe2
		}
	}
	if m.LogFormatEscaped {
		i--
		if m.LogFormatEscaped {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xd8
	}
	if m.RejectUnknownDynamicFields {
		i--
		if m.RejectUnknownDynamicFields {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xd0
	}
	if m.CpusetThreads {
		i--
		if m.CpusetThreads {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xc8
	}
	if m.RestartEpoch != 0 {
		i = encodeVarint(dAtA, i, uint64(m.RestartEpoch))
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xc0
	}
	if m.EnableMutexTracing {
		i--
		if m.EnableMutexTracing {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xb8
	}
	if m.DisableHotRestart {
		i--
		if m.DisableHotRestart {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0xb0
	}
	if m.Mode != 0 {
		i = encodeVarint(dAtA, i, uint64(m.Mode))
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0x98
	}
	if m.ParentShutdownTime != nil {
		size, err := (*durationpb.Duration)(m.ParentShutdownTime).MarshalToSizedBufferVTStrict(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarint(dAtA, i, uint64(size))
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0x92
	}
	if m.DrainTime != nil {
		size, err := (*durationpb.Duration)(m.DrainTime).MarshalToSizedBufferVTStrict(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarint(dAtA, i, uint64(size))
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0x8a
	}
	if m.FileFlushInterval != nil {
		size, err := (*durationpb.Duration)(m.FileFlushInterval).MarshalToSizedBufferVTStrict(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarint(dAtA, i, uint64(size))
		i--
		dAtA[i] = 0x1
		i--
		dAtA[i] = 0x82
	}
	if len(m.ServiceZone) > 0 {
		i -= len(m.ServiceZone)
		copy(dAtA[i:], m.ServiceZone)
		i = encodeVarint(dAtA, i, uint64(len(m.ServiceZone)))
		i--
		dAtA[i] = 0x7a
	}
	if len(m.ServiceNode) > 0 {
		i -= len(m.ServiceNode)
		copy(dAtA[i:], m.ServiceNode)
		i = encodeVarint(dAtA, i, uint64(len(m.ServiceNode)))
		i--
		dAtA[i] = 0x72
	}
	if len(m.ServiceCluster) > 0 {
		i -= len(m.ServiceCluster)
		copy(dAtA[i:], m.ServiceCluster)
		i = encodeVarint(dAtA, i, uint64(len(m.ServiceCluster)))
		i--
		dAtA[i] = 0x6a
	}
	if len(m.LogPath) > 0 {
		i -= len(m.LogPath)
		copy(dAtA[i:], m.LogPath)
		i = encodeVarint(dAtA, i, uint64(len(m.LogPath)))
		i--
		dAtA[i] = 0x5a
	}
	if len(m.LogFormat) > 0 {
		i -= len(m.LogFormat)
		copy(dAtA[i:], m.LogFormat)
		i = encodeVarint(dAtA, i, uint64(len(m.LogFormat)))
		i--
		dAtA[i] = 0x52
	}
	if len(m.ComponentLogLevel) > 0 {
		i -= len(m.ComponentLogLevel)
		copy(dAtA[i:], m.ComponentLogLevel)
		i = encodeVarint(dAtA, i, uint64(len(m.ComponentLogLevel)))
		i--
		dAtA[i] = 0x4a
	}
	if len(m.LogLevel) > 0 {
		i -= len(m.LogLevel)
		copy(dAtA[i:], m.LogLevel)
		i = encodeVarint(dAtA, i, uint64(len(m.LogLevel)))
		i--
		dAtA[i] = 0x42
	}
	if m.LocalAddressIpVersion != 0 {
		i = encodeVarint(dAtA, i, uint64(m.LocalAddressIpVersion))
		i--
		dAtA[i] = 0x38
	}
	if len(m.AdminAddressPath) > 0 {
		i -= len(m.AdminAddressPath)
		copy(dAtA[i:], m.AdminAddressPath)
		i = encodeVarint(dAtA, i, uint64(len(m.AdminAddressPath)))
		i--
		dAtA[i] = 0x32
	}
	if m.AllowUnknownStaticFields {
		i--
		if m.AllowUnknownStaticFields {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x28
	}
	if len(m.ConfigYaml) > 0 {
		i -= len(m.ConfigYaml)
		copy(dAtA[i:], m.ConfigYaml)
		i = encodeVarint(dAtA, i, uint64(len(m.ConfigYaml)))
		i--
		dAtA[i] = 0x22
	}
	if len(m.ConfigPath) > 0 {
		i -= len(m.ConfigPath)
		copy(dAtA[i:], m.ConfigPath)
		i = encodeVarint(dAtA, i, uint64(len(m.ConfigPath)))
		i--
		dAtA[i] = 0x1a
	}
	if m.Concurrency != 0 {
		i = encodeVarint(dAtA, i, uint64(m.Concurrency))
		i--
		dAtA[i] = 0x10
	}
	if m.BaseId != 0 {
		i = encodeVarint(dAtA, i, uint64(m.BaseId))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *ServerInfo) SizeVT() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Version)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	if m.State != 0 {
		n += 1 + sov(uint64(m.State))
	}
	if m.UptimeCurrentEpoch != nil {
		l = (*durationpb.Duration)(m.UptimeCurrentEpoch).SizeVT()
		n += 1 + l + sov(uint64(l))
	}
	if m.UptimeAllEpochs != nil {
		l = (*durationpb.Duration)(m.UptimeAllEpochs).SizeVT()
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.HotRestartVersion)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	if m.CommandLineOptions != nil {
		l = m.CommandLineOptions.SizeVT()
		n += 1 + l + sov(uint64(l))
	}
	if m.Node != nil {
		if size, ok := interface{}(m.Node).(interface {
			SizeVT() int
		}); ok {
			l = size.SizeVT()
		} else {
			l = proto.Size(m.Node)
		}
		n += 1 + l + sov(uint64(l))
	}
	n += len(m.unknownFields)
	return n
}

func (m *CommandLineOptions) SizeVT() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.BaseId != 0 {
		n += 1 + sov(uint64(m.BaseId))
	}
	if m.Concurrency != 0 {
		n += 1 + sov(uint64(m.Concurrency))
	}
	l = len(m.ConfigPath)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.ConfigYaml)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	if m.AllowUnknownStaticFields {
		n += 2
	}
	l = len(m.AdminAddressPath)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	if m.LocalAddressIpVersion != 0 {
		n += 1 + sov(uint64(m.LocalAddressIpVersion))
	}
	l = len(m.LogLevel)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.ComponentLogLevel)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.LogFormat)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.LogPath)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.ServiceCluster)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.ServiceNode)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	l = len(m.ServiceZone)
	if l > 0 {
		n += 1 + l + sov(uint64(l))
	}
	if m.FileFlushInterval != nil {
		l = (*durationpb.Duration)(m.FileFlushInterval).SizeVT()
		n += 2 + l + sov(uint64(l))
	}
	if m.DrainTime != nil {
		l = (*durationpb.Duration)(m.DrainTime).SizeVT()
		n += 2 + l + sov(uint64(l))
	}
	if m.ParentShutdownTime != nil {
		l = (*durationpb.Duration)(m.ParentShutdownTime).SizeVT()
		n += 2 + l + sov(uint64(l))
	}
	if m.Mode != 0 {
		n += 2 + sov(uint64(m.Mode))
	}
	if m.DisableHotRestart {
		n += 3
	}
	if m.EnableMutexTracing {
		n += 3
	}
	if m.RestartEpoch != 0 {
		n += 2 + sov(uint64(m.RestartEpoch))
	}
	if m.CpusetThreads {
		n += 3
	}
	if m.RejectUnknownDynamicFields {
		n += 3
	}
	if m.LogFormatEscaped {
		n += 3
	}
	if len(m.DisabledExtensions) > 0 {
		for _, s := range m.DisabledExtensions {
			l = len(s)
			n += 2 + l + sov(uint64(l))
		}
	}
	if m.IgnoreUnknownDynamicFields {
		n += 3
	}
	if m.UseDynamicBaseId {
		n += 3
	}
	l = len(m.BaseIdPath)
	if l > 0 {
		n += 2 + l + sov(uint64(l))
	}
	if m.DrainStrategy != 0 {
		n += 2 + sov(uint64(m.DrainStrategy))
	}
	if m.EnableFineGrainLogging {
		n += 3
	}
	l = len(m.SocketPath)
	if l > 0 {
		n += 2 + l + sov(uint64(l))
	}
	if m.SocketMode != 0 {
		n += 2 + sov(uint64(m.SocketMode))
	}
	if m.EnableCoreDump {
		n += 3
	}
	if len(m.StatsTag) > 0 {
		for _, s := range m.StatsTag {
			l = len(s)
			n += 2 + l + sov(uint64(l))
		}
	}
	n += len(m.unknownFields)
	return n
}
