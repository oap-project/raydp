# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import network_pb2 as network__pb2


class DriverServiceStub(object):
    """Driver Service definition
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RegisterWorker = channel.unary_unary(
                '/DriverService/RegisterWorker',
                request_serializer=network__pb2.RegisterWorkerRequest.SerializeToString,
                response_deserializer=network__pb2.RegisterWorkerReply.FromString,
                )
        self.RegisterWorkerService = channel.unary_unary(
                '/DriverService/RegisterWorkerService',
                request_serializer=network__pb2.RegisterWorkerServiceRequest.SerializeToString,
                response_deserializer=network__pb2.RegisterWorkerServiceReply.FromString,
                )
        self.RegisterFuncResult = channel.unary_unary(
                '/DriverService/RegisterFuncResult',
                request_serializer=network__pb2.FunctionResult.SerializeToString,
                response_deserializer=network__pb2.Empty.FromString,
                )


class DriverServiceServicer(object):
    """Driver Service definition
    """

    def RegisterWorker(self, request, context):
        """register the worker process to driver which used to tell the worker has started up
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RegisterWorkerService(self, request, context):
        """register the worker service host and port
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RegisterFuncResult(self, request, context):
        """register the function result
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DriverServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RegisterWorker': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterWorker,
                    request_deserializer=network__pb2.RegisterWorkerRequest.FromString,
                    response_serializer=network__pb2.RegisterWorkerReply.SerializeToString,
            ),
            'RegisterWorkerService': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterWorkerService,
                    request_deserializer=network__pb2.RegisterWorkerServiceRequest.FromString,
                    response_serializer=network__pb2.RegisterWorkerServiceReply.SerializeToString,
            ),
            'RegisterFuncResult': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterFuncResult,
                    request_deserializer=network__pb2.FunctionResult.FromString,
                    response_serializer=network__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'DriverService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class DriverService(object):
    """Driver Service definition
    """

    @staticmethod
    def RegisterWorker(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/DriverService/RegisterWorker',
            network__pb2.RegisterWorkerRequest.SerializeToString,
            network__pb2.RegisterWorkerReply.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RegisterWorkerService(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/DriverService/RegisterWorkerService',
            network__pb2.RegisterWorkerServiceRequest.SerializeToString,
            network__pb2.RegisterWorkerServiceReply.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RegisterFuncResult(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/DriverService/RegisterFuncResult',
            network__pb2.FunctionResult.SerializeToString,
            network__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)


class WorkerServiceStub(object):
    """Worker Service
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RunFunction = channel.unary_unary(
                '/WorkerService/RunFunction',
                request_serializer=network__pb2.Function.SerializeToString,
                response_deserializer=network__pb2.Empty.FromString,
                )
        self.Stop = channel.unary_unary(
                '/WorkerService/Stop',
                request_serializer=network__pb2.Empty.SerializeToString,
                response_deserializer=network__pb2.Empty.FromString,
                )


class WorkerServiceServicer(object):
    """Worker Service
    """

    def RunFunction(self, request, context):
        """run the given function
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Stop(self, request, context):
        """stop the worker service
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WorkerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RunFunction': grpc.unary_unary_rpc_method_handler(
                    servicer.RunFunction,
                    request_deserializer=network__pb2.Function.FromString,
                    response_serializer=network__pb2.Empty.SerializeToString,
            ),
            'Stop': grpc.unary_unary_rpc_method_handler(
                    servicer.Stop,
                    request_deserializer=network__pb2.Empty.FromString,
                    response_serializer=network__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'WorkerService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class WorkerService(object):
    """Worker Service
    """

    @staticmethod
    def RunFunction(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/WorkerService/RunFunction',
            network__pb2.Function.SerializeToString,
            network__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Stop(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/WorkerService/Stop',
            network__pb2.Empty.SerializeToString,
            network__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
