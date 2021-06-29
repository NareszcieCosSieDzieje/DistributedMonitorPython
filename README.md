# DistributedMonitorPython

- This library provides mechanisms for multi-host synchronization.

- The underlying algorithm for handling critical sections is "Lamport's Mutual Exclusion Algorithm for Distributed Systems."

## Initialization

- The hosts file:
```
host_dict = {
                'main': {'ip': '127.0.0.1', 'port': 5555},
                'other': [
                            {'ip': '127.0.0.1', 'port': 5556},
                            {'ip': '127.0.0.1', 'port': 5557}
                         ]
            }
```
    1. Passed as an initializing paremeter for the DstributedMonitor class.
    2. Requires both the 'main' and 'other' keys.
    3. Each port should be unique per host.
    4. The value behind the 'main' key should be a dictionary with an 'ip' and a unique 'port'. 
    5. The value behind the 'main' key should be a list of dictionaries.

- Initializing the class:
```
distributed_monitor = DistributedMonitor(hosts=host_dict)
```

## Working with the monitor instance

```
# ... some code here 
distributed_monitor.begin_synchronized()
print('x = 1')
# e.g.: send x value to other hosts (handled by the programmer)
distributed_monitor.signal_all()
distributed_monitor.block()
print('y = 2')
distributed_monitor.signal()
time.sleep(2)
distributed_monitor.end_synchronized()
```
    1. begin_synchronized() -> block until the host acquires the critical section.
    2. end-synchronized() -> releases the acquired critical section.
    3. block() -> releases the critical section, and blocks until another host sends a signal and releases the critical section. Afterwards fights to re-acquire the critical section. 
    4. signal() -> sends a signal to some arbitrary host that is in the blocking state.
    5. signal_all() -> same as signal, but sends messages to all blocking hosts.