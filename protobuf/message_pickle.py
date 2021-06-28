import sys
from enum import Enum

from . import lamport_pb2


class MessageTypes(Enum):
    REQUEST_INITIAL = 0
    REPLY_INITIAL = 1
    RELEASE_INITIAL = 2
    REQUEST = 3
    REPLY = 4
    RELEASE = 5
    SLEEP = 6
    WAKE = 7
    END_COMMUNICATION = 8


def serialize_lamport(message_type: MessageTypes, clock: int, id_sender: int, id_receiver: int) -> str:  # TODO: DODAC POLE DATA?

    lamport_class = lamport_pb2.Lamport()

    if message_type == MessageTypes.REQUEST_INITIAL:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.REQUEST_INITIAL
    elif message_type == MessageTypes.REPLY_INITIAL:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.REPLY_INITIAL
    elif message_type == MessageTypes.RELEASE_INITIAL:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.RELEASE_INITIAL
    elif message_type == MessageTypes.REQUEST:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.REQUEST
    elif message_type == MessageTypes.REPLY:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.REPLY
    elif message_type == MessageTypes.RELEASE:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.RELEASE
    elif message_type == MessageTypes.SLEEP:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.SLEEP
    elif message_type == MessageTypes.WAKE:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.WAKE
    elif message_type == MessageTypes.END_COMMUNICATION:
        lamport_class.message_type = lamport_pb2.Lamport.MessageType.END_COMMUNICATION
    else:
        raise ValueError('Message type not one of the available types.')
    if 0 <= clock < sys.maxsize:
        lamport_class.clock = clock
    else:
        raise OverflowError('Clock value out of range.')
    if 0 <= id_sender < sys.maxsize:
        lamport_class.id_sender = id_sender
    else:
        raise OverflowError('Sender ID out of range.')
    #if 0 <= id_receiver < sys.maxsize:
    if id_receiver < sys.maxsize:
        lamport_class.id_receiver = id_receiver
    else:
        raise OverflowError(f'Receiver ID out of range {id_receiver}.')
    return lamport_class.SerializeToString()


def deserialize_lamport(serialized_lamport_message):
    lamport_class = lamport_pb2.Lamport()
    lamport_class.ParseFromString(serialized_lamport_message)
    return lamport_class


if __name__ == '__main__':
    print(f'data: {MessageTypes.REQUEST_INITIAL}, {1}, {101}, {201}')
    serialized_data = serialize_lamport(MessageTypes.REQUEST_INITIAL, 1, 101, 201)
    print(f'serialized_data: {serialized_data}')
    deserialized_data = deserialize_lamport(serialized_data)
    print('type', type(deserialized_data))
    print(f'deserialized_data: {deserialized_data}')
