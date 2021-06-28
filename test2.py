from distributed_monitor import DistributedMonitor

import time
print('start 2')
host_dict = {
                'main': {'ip': '127.0.0.1', 'port': 5556},
                'other': [
                            {'ip': '127.0.0.1', 'port': 5555},
                            {'ip': '127.0.0.1', 'port': 5557}
                         ]
            }

# localhost -> '127.0.0.1'


distributed_monitor = DistributedMonitor(hosts=host_dict)

time.sleep(10)
distributed_monitor.begin_synchronized()
print('x = 2')
time.sleep(1)
distributed_monitor.signal()
distributed_monitor.end_synchronized()
# while True:
#     pass
print('sleep')
time.sleep(120)


print('end 2')