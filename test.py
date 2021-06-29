from distributed_monitor import DistributedMonitor
import time
# from protobuf.message_pickle import MessageTypes, serialize_lamport, deserialize_lamport
# print(MessageTypes.REQUEST_INITIAL.value)
#
# print(f'data: {MessageTypes.REQUEST_INITIAL}, {1}, {101}, {201}')
# serialized_data = serialize_lamport(MessageTypes.REQUEST_INITIAL, 1, 101, 201)
# print(f'serialized_data: {serialized_data}')
# deserialized_data = deserialize_lamport(serialized_data)
# # print('type', dir(deserialized_data))
# print(f'deserialized_data: {deserialized_data}')

print('start 1')
host_dict = {
                'main': {'ip': '127.0.0.1', 'port': 5555},
                'other': [
                            {'ip': '127.0.0.1', 'port': 5556},
                            {'ip': '127.0.0.1', 'port': 5557}
                         ]
            }

# localhost -> '127.0.0.1'

distributed_monitor = DistributedMonitor(hosts=host_dict)

distributed_monitor.begin_synchronized()
print('x = 1')
print('y = 11')
time.sleep(2)
distributed_monitor.end_synchronized()

# while True:
#     pass
# OBSLUZ KILKA NA RAZ ITP
print('sleep')
# time.sleep(120)


print('end 1')