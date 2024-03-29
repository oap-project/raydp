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
  serialized_pb=b'\n\rnetwork.proto\";\n\x15RegisterWorkerRequest\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x12\n\nworld_rank\x18\x02 \x01(\x05\"-\n\x13RegisterWorkerReply\x12\x16\n\x0enode_addresses\x18\x03 \x03(\t\"Z\n\x1cRegisterWorkerServiceRequest\x12\x12\n\nworld_rank\x18\x01 \x01(\x05\x12\x11\n\tworker_ip\x18\x02 \x01(\t\x12\x13\n\x0bworker_port\x18\x03 \x01(\x05\"I\n\x1aRegisterWorkerServiceReply\x12\x13\n\x0bray_address\x18\x01 \x01(\t\x12\x16\n\x0eredis_password\x18\x02 \x01(\t\")\n\x08\x46unction\x12\x0f\n\x07\x66unc_id\x18\x01 \x01(\x05\x12\x0c\n\x04\x66unc\x18\x02 \x01(\x0c\"E\n\x0e\x46unctionResult\x12\x12\n\nworld_rank\x18\x01 \x01(\x05\x12\x0f\n\x07\x66unc_id\x18\x02 \x01(\x05\x12\x0e\n\x06result\x18\x03 \x01(\x0c\"\x07\n\x05\x45mpty2\xd3\x01\n\rDriverService\x12>\n\x0eRegisterWorker\x12\x16.RegisterWorkerRequest\x1a\x14.RegisterWorkerReply\x12S\n\x15RegisterWorkerService\x12\x1d.RegisterWorkerServiceRequest\x1a\x1b.RegisterWorkerServiceReply\x12-\n\x12RegisterFuncResult\x12\x0f.FunctionResult\x1a\x06.Empty2I\n\rWorkerService\x12 \n\x0bRunFunction\x12\t.Function\x1a\x06.Empty\x12\x16\n\x04Stop\x12\x06.Empty\x1a\x06.Emptyb\x06proto3'
)




_REGISTERWORKERREQUEST = _descriptor.Descriptor(
  name='RegisterWorkerRequest',
  full_name='RegisterWorkerRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='job_id', full_name='RegisterWorkerRequest.job_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='world_rank', full_name='RegisterWorkerRequest.world_rank', index=1,
      number=2, type=5, cpp_type=1, label=1,
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
  serialized_start=17,
  serialized_end=76,
)


_REGISTERWORKERREPLY = _descriptor.Descriptor(
  name='RegisterWorkerReply',
  full_name='RegisterWorkerReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='node_addresses', full_name='RegisterWorkerReply.node_addresses', index=0,
      number=3, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=78,
  serialized_end=123,
)


_REGISTERWORKERSERVICEREQUEST = _descriptor.Descriptor(
  name='RegisterWorkerServiceRequest',
  full_name='RegisterWorkerServiceRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='world_rank', full_name='RegisterWorkerServiceRequest.world_rank', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='worker_ip', full_name='RegisterWorkerServiceRequest.worker_ip', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='worker_port', full_name='RegisterWorkerServiceRequest.worker_port', index=2,
      number=3, type=5, cpp_type=1, label=1,
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
  serialized_start=125,
  serialized_end=215,
)


_REGISTERWORKERSERVICEREPLY = _descriptor.Descriptor(
  name='RegisterWorkerServiceReply',
  full_name='RegisterWorkerServiceReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='ray_address', full_name='RegisterWorkerServiceReply.ray_address', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='redis_password', full_name='RegisterWorkerServiceReply.redis_password', index=1,
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
  serialized_start=217,
  serialized_end=290,
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
  serialized_start=292,
  serialized_end=333,
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
      name='world_rank', full_name='FunctionResult.world_rank', index=0,
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
  serialized_start=335,
  serialized_end=404,
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
  serialized_start=406,
  serialized_end=413,
)

DESCRIPTOR.message_types_by_name['RegisterWorkerRequest'] = _REGISTERWORKERREQUEST
DESCRIPTOR.message_types_by_name['RegisterWorkerReply'] = _REGISTERWORKERREPLY
DESCRIPTOR.message_types_by_name['RegisterWorkerServiceRequest'] = _REGISTERWORKERSERVICEREQUEST
DESCRIPTOR.message_types_by_name['RegisterWorkerServiceReply'] = _REGISTERWORKERSERVICEREPLY
DESCRIPTOR.message_types_by_name['Function'] = _FUNCTION
DESCRIPTOR.message_types_by_name['FunctionResult'] = _FUNCTIONRESULT
DESCRIPTOR.message_types_by_name['Empty'] = _EMPTY
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

RegisterWorkerRequest = _reflection.GeneratedProtocolMessageType('RegisterWorkerRequest', (_message.Message,), {
  'DESCRIPTOR' : _REGISTERWORKERREQUEST,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:RegisterWorkerRequest)
  })
_sym_db.RegisterMessage(RegisterWorkerRequest)

RegisterWorkerReply = _reflection.GeneratedProtocolMessageType('RegisterWorkerReply', (_message.Message,), {
  'DESCRIPTOR' : _REGISTERWORKERREPLY,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:RegisterWorkerReply)
  })
_sym_db.RegisterMessage(RegisterWorkerReply)

RegisterWorkerServiceRequest = _reflection.GeneratedProtocolMessageType('RegisterWorkerServiceRequest', (_message.Message,), {
  'DESCRIPTOR' : _REGISTERWORKERSERVICEREQUEST,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:RegisterWorkerServiceRequest)
  })
_sym_db.RegisterMessage(RegisterWorkerServiceRequest)

RegisterWorkerServiceReply = _reflection.GeneratedProtocolMessageType('RegisterWorkerServiceReply', (_message.Message,), {
  'DESCRIPTOR' : _REGISTERWORKERSERVICEREPLY,
  '__module__' : 'network_pb2'
  # @@protoc_insertion_point(class_scope:RegisterWorkerServiceReply)
  })
_sym_db.RegisterMessage(RegisterWorkerServiceReply)

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
  serialized_start=416,
  serialized_end=627,
  methods=[
  _descriptor.MethodDescriptor(
    name='RegisterWorker',
    full_name='DriverService.RegisterWorker',
    index=0,
    containing_service=None,
    input_type=_REGISTERWORKERREQUEST,
    output_type=_REGISTERWORKERREPLY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='RegisterWorkerService',
    full_name='DriverService.RegisterWorkerService',
    index=1,
    containing_service=None,
    input_type=_REGISTERWORKERSERVICEREQUEST,
    output_type=_REGISTERWORKERSERVICEREPLY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='RegisterFuncResult',
    full_name='DriverService.RegisterFuncResult',
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
  serialized_start=629,
  serialized_end=702,
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
