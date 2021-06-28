from distributed_monitor import DistributedMonitor
import time
print('start 3')
host_dict = {
                'main': {'ip': '127.0.0.1', 'port': 5557},
                'other': [
                            {'ip': '127.0.0.1', 'port': 5555},
                            {'ip': '127.0.0.1', 'port': 5556}
                         ]
            }

# localhost -> '127.0.0.1'

distributed_monitor = DistributedMonitor(hosts=host_dict)

distributed_monitor.begin_synchronized()
print('x = 3')
time.sleep(5)
distributed_monitor.signal()
distributed_monitor.signal()
distributed_monitor.end_synchronized()

# while True:
#     pass
print('sleep')
time.sleep(120)

print('end 3')