# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: network.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='network.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\rnetwork.proto\"E\n\x14\x41gentRegisterRequest\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x07\x63ommand\x18\x03 \x01(\t\"%\n\x12\x41gentRegisterReply\x12\x0f\n\x07succeed\x18\x01 \x01(\x08\"s\n\x15WorkerRegisterRequest\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x0f\n\x07rank_id\x18\x02 \x01(\x05\x12\x11\n\tpeer_name\x18\x03 \x01(\t\x12\x11\n\tworker_ip\x18\x04 \x01(\t\x12\x13\n\x0bworker_port\x18\x05 \x01(\x05\"B\n\x13WorkerRegisterReply\x12\x13\n\x0bray_address\x18\x01 \x01(\t\x12\x16\n\x0eredis_password\x18\x02 \x01(\t\")\n\x08\x46unction\x12\x0f\n\x07\x66unc_id\x18\x01 \x01(\x05\x12\x0c\n\x04\x66unc\x18\x02 \x01(\x0c\"X\n\x0e\x46unctionResult\x12\x0f\n\x07rank_id\x18\x01 \x01(\x05\x12\x0f\n\x07\x66unc_id\x18\x02 \x01(\x05\x12\x0e\n\x06result\x18\x03 \x01(\x0c\x12\x14\n\x0cis_exception\x18\x04 \x01(\x08\"\x07\n\x05\x45mpty2\xbb\x01\n\rDriverService\x12;\n\rRegisterAgent\x12\x15.AgentRegisterRequest\x1a\x13.AgentRegisterReply\x12>\n\x0eRegisterWorker\x12\x16.WorkerRegisterRequest\x1a\x14.WorkerRegisterReply\x12-\n\x12SendFunctionResult\x12\x0f.FunctionResult\x1a\x06.Empty2I\n\rWorkerService\x12 \n\x0bRunFunction\x12\t.Function\x1a\x06.Empty\x12\x16\n\x04Stop\x12\x06.Empty\x1a\x06.Emptyb\x06proto3'
)




_AGENTREGISTERREQUEST = _descriptor.Descriptor(
  name='AgentRegisterRequest',
  full_name='AgentRegisterRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='job_id', full_name='AgentRegisterRequest.job_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='name', full_name='AgentRegisterRequest.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='command', full_name='AgentRegisterRequest.command', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=17,
  serialized_end=86,
)


_AGENTREGISTERREPLY = _descriptor.Descriptor(
  name='AgentRegisterReply',
  full_name='AgentRegisterReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='succeed', full_name='AgentRegisterReply.succeed', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=88,
  serialized_end=125,
)


_WORKERREGISTERREQUEST = _descriptor.Descriptor(
  name='WorkerRegisterRequest',
  full_name='WorkerRegisterRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='job_id', full_name='WorkerRegisterRequest.job_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='rank_id', full_name='WorkerRegisterRequest.rank_id', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='peer_name', full_name='WorkerRegisterRequest.peer_name', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='worker_ip', full_name='WorkerRegisterRequest.worker_ip', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='worker_port', full_name='WorkerRegisterRequest.worker_port', index=4,
      number=5, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=127,
  serialized_end=242,
)


_WORKERREGISTERREPLY = _descriptor.Descriptor(
  name='WorkerRegisterReply',
  full_name='WorkerRegisterReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='ray_address', full_name='WorkerRegisterReply.ray_address', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='redis_password', full_name='WorkerRegisterReply.redis_password', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=244,
  serialized_end=310,
)


_FUNCTION = _descriptor.Descriptor(
  name='Function',
  full_name='Function',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='func_id', full_name='Function.func_id', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='func', full_name='Function.func', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=312,
  serialized_end=353,
)


_FUNCTIONRESULT = _descriptor.Descriptor(
  name='FunctionResult',
  full_name='FunctionResult',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='rank_id', full_name='FunctionResult.rank_id', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='func_id', full_name='FunctionResult.func_id', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='result', full_name='FunctionResult.result', index=2,
      number=3, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='is_exception', full_name='FunctionResult.is_exception', index=3,
      number=4, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=355,
  serialized_end=443,
)


_EMPTY = _descriptor.Descriptor(
  name='Empty',
  full_name='Empty',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=445,
  serialized_end=452,
)

DESCRIPTOR.message_types_by_name['AgentRegisterRequest'] = _AGENTREGISTERREQUEST
DESCRIPTOR.message_types_by_name['AgentRegisterReply'] = _AGENTREGISTERREPLY
DESCRIPTOR.message_types_by_name['WorkerRegisterRequest'] = _WORKERREGISTERREQUEST
DESCRIPTOR.message_types_by_name['WorkerRegisterReply'] = _WORKERREGISTERREPLY
DESCRIPTOR.message_types_by_name['Function'] = _FUNCTION
DESCRIPTOR.message_types_by_name['FunctionResult'] = _FUNCTIONRESULT
DESCRIPTOR.message_types_by_name['Empty'] = _EMPTY
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

AgentRegisterRequest = _reflection.GeneratedProtocolMessageType('AgentRegisterRequest', (_message.Message,), {
  'DESCRIPTOR' : _AGENTREGISTERREQUEST,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:AgentRegisterRequest)
  })
_sym_db.RegisterMessage(AgentRegisterRequest)

AgentRegisterReply = _reflection.GeneratedProtocolMessageType('AgentRegisterReply', (_message.Message,), {
  'DESCRIPTOR' : _AGENTREGISTERREPLY,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:AgentRegisterReply)
  })
_sym_db.RegisterMessage(AgentRegisterReply)

WorkerRegisterRequest = _reflection.GeneratedProtocolMessageType('WorkerRegisterRequest', (_message.Message,), {
  'DESCRIPTOR' : _WORKERREGISTERREQUEST,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:WorkerRegisterRequest)
  })
_sym_db.RegisterMessage(WorkerRegisterRequest)

WorkerRegisterReply = _reflection.GeneratedProtocolMessageType('WorkerRegisterReply', (_message.Message,), {
  'DESCRIPTOR' : _WORKERREGISTERREPLY,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:WorkerRegisterReply)
  })
_sym_db.RegisterMessage(WorkerRegisterReply)

Function = _reflection.GeneratedProtocolMessageType('Function', (_message.Message,), {
  'DESCRIPTOR' : _FUNCTION,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:Function)
  })
_sym_db.RegisterMessage(Function)

FunctionResult = _reflection.GeneratedProtocolMessageType('FunctionResult', (_message.Message,), {
  'DESCRIPTOR' : _FUNCTIONRESULT,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:FunctionResult)
  })
_sym_db.RegisterMessage(FunctionResult)

Empty = _reflection.GeneratedProtocolMessageType('Empty', (_message.Message,), {
  'DESCRIPTOR' : _EMPTY,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:Empty)
  })
_sym_db.RegisterMessage(Empty)



_DRIVERSERVICE = _descriptor.ServiceDescriptor(
  name='DriverService',
  full_name='DriverService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=455,
  serialized_end=642,
  methods=[
  _descriptor.MethodDescriptor(
    name='RegisterAgent',
    full_name='DriverService.RegisterAgent',
    index=0,
    containing_service=None,
    input_type=_AGENTREGISTERREQUEST,
    output_type=_AGENTREGISTERREPLY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='RegisterWorker',
    full_name='DriverService.RegisterWorker',
    index=1,
    containing_service=None,
    input_type=_WORKERREGISTERREQUEST,
    output_type=_WORKERREGISTERREPLY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='SendFunctionResult',
    full_name='DriverService.SendFunctionResult',
    index=2,
    containing_service=None,
    input_type=_FUNCTIONRESULT,
    output_type=_EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_DRIVERSERVICE)

DESCRIPTOR.services_by_name['DriverService'] = _DRIVERSERVICE


_WORKERSERVICE = _descriptor.ServiceDescriptor(
  name='WorkerService',
  full_name='WorkerService',
  file=DESCRIPTOR,
  index=1,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=644,
  serialized_end=717,
  methods=[
  _descriptor.MethodDescriptor(
    name='RunFunction',
    full_name='WorkerService.RunFunction',
    index=0,
    containing_service=None,
    input_type=_FUNCTION,
    output_type=_EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='Stop',
    full_name='WorkerService.Stop',
    index=1,
    containing_service=None,
    input_type=_EMPTY,
    output_type=_EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_WORKERSERVICE)

DESCRIPTOR.services_by_name['WorkerService'] = _WORKERSERVICE

# @@protoc_insertion_point(module_scope)
