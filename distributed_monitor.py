from protobuf.message_pickle import MessageTypes, serialize_lamport, deserialize_lamport
import sys
import threading
import zmq
import time
import random
import logging
from queue import PriorityQueue, Queue

# sys.path.append(".\protobuf")


# DEBUG
# from hanging_threads import start_monitoring
# start_monitoring(seconds_frozen=40, test_interval=100)

logging.basicConfig(level=logging.DEBUG)


class HostsDictError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class DistributedMonitor:

    max_id = 0
    BROADCAST_MESSAGE = -10

    def __init__(self,  hosts: dict):
        """
        hosts dict example:
        {
        "main": {"ip": "localhost", "port": 5543}
        "others": [
                    {"ip": "124.214.124.142", "port": 5322},
                    ...
                  ]
        }
        """
        DistributedMonitor.max_id += 1
        self._instance_id = DistributedMonitor.max_id  # Could identify the monitor host set in connection
        self._lamport_clock = 0  # Lamport clock used for synchronizing critical section access
        self._lamport_lock = threading.Condition()

        self._synchronization_phase = True
        self.__synchronization_loop = True
        self._synchronization_phase_lock = threading.Condition()

        self._in_critical_section = False
        self._in_critical_section_lock = threading.Condition()

        self._asleep = False
        self._asleep_lock = threading.Condition()

        self._release_cond = False
        self._release_cond_lock = threading.Condition()

        self._instance_dead = False
        self._instance_dead_lock = threading.Condition()

        if 'main' not in hosts:
            logging.info(f'Host cannot be initialized. The key \'main\' is missing from the input \'hosts\' dictionary.\nExiting.')
            raise HostsDictError(-1)
        elif 'other' not in hosts:
            logging.info(f'Host cannot be initialized. The key \'others\' is missing from the input \'hosts\' dictionary.\nExiting.')
            raise HostsDictError(-2)
        elif len(hosts['other']) == 0:
            logging.info(f'Host cannot be initialized. The list of other hosts (\'others\') is empty.\nExiting.')
            raise HostsDictError(-3)
        elif 'ip' not in hosts['main']:
            logging.info(f'Host cannot be initialized. The key \'ip\' is missing from the \'main\' dictionary.\nExiting.')
            raise HostsDictError(-4)
        elif 'port' not in hosts['main']:
            logging.info(f'Host cannot be initialized. The key \'port\' is missing from the \'main\' dictionary.\nExiting.')
            raise HostsDictError(-5)
        for other_host in hosts['other']:
            if 'ip' not in other_host:
                logging.info(f'Host cannot be initialized. The key \'ip\' is missing from [{other_host}] in the \'others\' list.\nExiting.')
                raise HostsDictError(-6)
            elif 'port' not in other_host:
                logging.info(f'Host cannot be initialized. The key \'port\' is missing from [{other_host}] in the \'others\' list.\nExiting.')
                raise HostsDictError(-7)

        # HOSTS IDENTIFIED BY UNIQUE PORT NUMBERS
        self.host_id = hosts['main']['port']
        self.other_host_ids = [other_host['port'] for other_host in hosts['other']]
        self._other_host_ids_lock = threading.Condition()

        number_of_other_hosts = len(hosts['other'])

        all_ids = [self.host_id] + self.other_host_ids
        seen_ports = set()
        repeated_ports = set(port for port in all_ids if port in seen_ports or seen_ports.add(port))

        if len(repeated_ports) > 0:
            logging.info(f'Host cannot be initialized. Ports have to be unique per host. List of repeated ports: {repeated_ports}')
            raise HostsDictError(-8)

        self._all_hosts = {}
        for host_dict in hosts['other'] + [hosts['main']]:
            host_dict['port'] = int(host_dict['port'])
            if not (1023 < host_dict['port'] <= 65535):
                logging.info(f'Host cannot be initialized. [{host_dict}]. Ports have to be within the range (1023 < port <= 65535).')
                raise HostsDictError(-9)
            host_dict_id = host_dict['port']
            self._all_hosts[host_dict_id] = host_dict

        self._hosts_acks = dict(zip(self.other_host_ids, [0]*len(self.other_host_ids)))
        self._hosts_acks_lock = threading.Condition()

        self._hosts_replies = dict(zip(self.other_host_ids, [False]*len(self.other_host_ids)))
        self._hosts_replies_lock = threading.Condition()

        self._critical_section_queue = PriorityQueue(maxsize=number_of_other_hosts+1)
        self._sleep_set = set()
        self._sleep_set_lock = threading.Condition()

        # self.shared_variables = {}
        # COULD EXTEND THE FUNCTIONALITY OF THE MODULE BY PROVIDING A WAY OF SYNCHRONIZING VARIABLES

        self._should_signal = 0
        self._should_signal_lock = threading.Condition()

        self._zmq_context = zmq.Context()
        self.publisher_socket = self._zmq_context.socket(zmq.PUB)
        self.publisher_socket.bind(f'tcp://{self._all_hosts[self.host_id]["ip"]}:{self._all_hosts[self.host_id]["port"]}')

        self.subscriber_socket = self._zmq_context.socket(zmq.SUB)
        for other_host_id in self.other_host_ids:
            self.subscriber_socket.connect(f'tcp://{self._all_hosts[other_host_id]["ip"]}:{self._all_hosts[other_host_id]["port"]}')

        self._subscriber_loop = True
        self._subscriber_loop_lock = threading.Condition()

        self._poller = zmq.Poller()
        self._poller.register(self.subscriber_socket, zmq.POLLIN)
        self.subscriber_socket.subscribe("")  # setsockopt_string(zmq.SUBSCRIBE, "")

        self.subscriber_thread = threading.Thread(target=self._subscriber, args=())
        self.subscriber_thread.setDaemon(True)

        self.subscriber_thread.start()
        self._synchronize_hosts()

    def _synchronize_hosts(self):
        """ Synchronizes the hosts to make sure they will receive messages.
        Sends messages in three phases, stepping through them
        via synchronization with the _subscriber thread.
        Steps:
            1. REQUEST_INITIAL
            2. REPLY_INITIAL
            3. RELEASE_INITIAL
        """
        # REQ -> PHASE ONE
        synchronization_loop = True
        while synchronization_loop:
            with self._other_host_ids_lock:
                for destination_host in self.other_host_ids:
                    self._send(MessageTypes.REQUEST_INITIAL, destination_host)
            logging.debug('Sent 1. REQUEST_INITIAL.')
            with self._hosts_acks_lock:
                if len(self._hosts_acks) > 0 and all(map(lambda x: True if x > 0 else False, self._hosts_acks.values())):
                    # REP -> PHASE TWO
                    logging.debug('Sent 2. REPLY_INITIAL.')
                    with self._other_host_ids_lock:
                        for destination_host in self.other_host_ids:
                            self._send(MessageTypes.REPLY_INITIAL, destination_host)
            with self._hosts_acks_lock:
                if len(self._hosts_acks) > 0 and all(map(lambda x: True if x == 2 else False, self._hosts_acks.values())):
                    break
            with self._synchronization_phase_lock:
                self._synchronization_loop = False
            time.sleep(1)  # TODO: OBCZAJ
        # REL -> PHASE THREE
        logging.debug('Sent 3. RELEASE_INITIAL.')
        with self._other_host_ids_lock:
            for destination_host in self.other_host_ids:
                self._send(MessageTypes.RELEASE_INITIAL, destination_host)
        # END
        with self._synchronization_phase_lock:
            self._synchronization_phase = False

    # COULD BE USED IN THE FUTURE
    # def update_shared_resource(self, resource_key, resource_val):
    #     self.shared_variables.update({resource_key: resource_val})

    def _subscriber(self):
        logging.debug(f'Host[{self.host_id}]\'s subscriber thread started.')
        # POLL provides timeouts, scales good with more sockets.
        subscriber_loop = True
        while subscriber_loop:
            # ADDED TIMEOUT TO CHECK THE SUBSCRIBER LOOP COND VAR IN CASE OF NO INCOMING MESSAGES (blocked socket)
            available_sockets = dict(self._poller.poll(1000))  # 1000  WAIT 1000ms == 1s
            if self.subscriber_socket in available_sockets and available_sockets[self.subscriber_socket] == zmq.POLLIN:
                try:
                    self._receive()
                except Exception as e:
                    logging.warning(f'Error while receiving {e}')
            with self._subscriber_loop_lock:
                subscriber_loop = self._subscriber_loop
        logging.debug(f'Host[{self.host_id}]\'s subscriber thread exiting.')

    def _send(self, message_type: MessageTypes, id_receiver: int):
        with self._lamport_lock:
            current_clock = self._lamport_clock
        serialized_message = serialize_lamport(message_type, current_clock, self.host_id, id_receiver)
        self.publisher_socket.send(serialized_message)
        with self._lamport_lock:
            self._lamport_clock += 1

    def _broadcast(self, message_type: MessageTypes):
        self._send(message_type, DistributedMonitor.BROADCAST_MESSAGE)

    def _receive(self):
        received_msg_raw = self.subscriber_socket.recv()
        received_msg_deserialized = deserialize_lamport(received_msg_raw)

        receiver_id = int(received_msg_deserialized.id_receiver)
        message_type = received_msg_deserialized.message_type

        # LISTEN TO ALL WAKE SIGNALS TO MONITOR THE STATE OF ALL SLEEPING HOST
        if receiver_id == self.host_id or receiver_id == DistributedMonitor.BROADCAST_MESSAGE or message_type == MessageTypes.WAKE.value:
            sender_id = int(received_msg_deserialized.id_sender)
            sender_clock = int(received_msg_deserialized.clock)

            with self._lamport_lock:
                self._lamport_clock = max(self._lamport_clock, sender_clock) + 1

            # TODO: SPAWN THREAD TO HANDLE THE MESSAGE? Not needed, no handle should block or take too long
            with self._synchronization_phase_lock:
                if self._synchronization_phase:
                    if message_type == MessageTypes.REQUEST_INITIAL.value:
                        self._handle_initial_request_msg(sender_id)
                    elif message_type == MessageTypes.REPLY_INITIAL.value:
                        self._handle_initial_reply_msg(sender_id)
                    elif message_type == MessageTypes.RELEASE_INITIAL.value:
                        self._handle_initial_release_msg()
            if message_type == MessageTypes.REQUEST.value:
                self._handle_request_msg(sender_id, sender_clock)
            elif message_type == MessageTypes.REPLY.value:
                self._handle_reply_msg(sender_id, sender_clock)
            elif message_type == MessageTypes.RELEASE.value:
                # FUTURE OPTION? UPDATE SHARED_VARIABLES MAP
                self._handle_release_msg(sender_id)
            elif message_type == MessageTypes.SLEEP.value:
                self._handle_sleep_msg(sender_id)
            elif message_type == MessageTypes.WAKE.value:
                self._handle_wake_msg(receiver_id)
            elif message_type == MessageTypes.END_COMMUNICATION.value:
                self._handle_end_communication(sender_id)
        else:
            logging.debug(f'Msg dropped from {received_msg_deserialized.id_sender} for {receiver_id}')

    def _hosts_ack_handler(self, sender_id: int, status: int):
        with self._hosts_acks_lock:
            if sender_id in self._hosts_acks:
                self._hosts_acks[sender_id] = status

    def _handle_initial_request_msg(self, sender_id: int):
        self._hosts_ack_handler(sender_id, 1)

    def _handle_initial_reply_msg(self, sender_id: int):
        self._hosts_ack_handler(sender_id, 2)

    def _handle_initial_release_msg(self):
        # Unconditionally exit the sync phase
        status = 2
        with self._hosts_acks_lock:
            self._hosts_acks = {host_id: status for host_id in self._hosts_acks}

    def _handle_request_msg(self, sender_id: int, sender_clock: int):
        logging.debug(f'Host[{self.host_id}] handling critical section (REQUEST) from host[{sender_id}].')
        self._reply(sender_id, sender_clock)
        # When sending a reply assume someone might enter the cs

    def _handle_reply_msg(self, sender_id: int, sender_clock: int):
        logging.debug(f'Host[{self.host_id}] handling critical section (REPLY) from host[{sender_id}]:clock({sender_clock}).')
        with self._hosts_replies_lock:
            if sender_id in self._hosts_replies:
                self._hosts_replies[sender_id] = True
                if all(self._hosts_replies.values()):
                    if self.host_id == self._critical_section_queue.queue[0][1]:
                        with self._in_critical_section_lock:
                            self._in_critical_section = True
                            logging.debug(f'Host[{self.host_id}] entered the critical section.')
                            self._in_critical_section_lock.notifyAll()
                    for host_id in self._hosts_replies:
                        self._hosts_replies[host_id] = False

    def _handle_release_msg(self, sender_id: int):
        logging.debug(f'Host[{self.host_id}] handling critical section (RELEASE) from host[{sender_id}].')
        (used_sender_clock, used_sender_id) = self._critical_section_queue.get()
        with self._release_cond_lock:
            self._release_cond = False
            self._release_cond_lock.notifyAll()
        if len(self._critical_section_queue.queue) > 0:
            if self.host_id == self._critical_section_queue.queue[0][1]:  # TODO: CHECK WHETHER THE QUEUE LOOKUP IS THREAD-SAFE?
                with self._in_critical_section_lock:
                    self._in_critical_section = True
                    logging.debug(f'Host[{self.host_id}] entered the critical section.')
                    self._in_critical_section_lock.notifyAll()
            if int(used_sender_id) != int(sender_id):
                # This should never happen.
                print(f'{type(used_sender_id)} | {type(sender_id)}')
                logging.critical(f'Handle release msg id error. {used_sender_id} != {sender_id}')

    def _handle_end_communication(self, sender_id: int):
        # CAN HAPPEN WHENEVER
        logging.info(f'Host[{self.host_id}] handling (END-COMMUNICATION) from host[{sender_id}].')

        sender_ip = self._all_hosts[sender_id]["ip"]
        sender_port = self._all_hosts[sender_id]["port"]
        self._all_hosts.pop(sender_id)

        with self._other_host_ids_lock:
            self.other_host_ids.remove(sender_id)
            number_of_other_hosts = len(self.other_host_ids)

        # IF ONE OF TWO ENDS, THE OTHER ENDS TOO
        if number_of_other_hosts == 0:
            logging.info(f'Host[{self.host_id}] is exiting due to all other hosts having exited.')
            self.stop_monitor()
            # KONIEC CZY TO ZADZIALA??

        cs_items = []
        sender_had_cs = False
        i = 0
        while not self._critical_section_queue.empty():
            cs_item = self._critical_section_queue.get()
            (used_sender_clock, used_sender_id) = cs_item
            if i == 0 and used_sender_id == sender_id:
                # IF HAD SECTION HANDLE RELEASE ON THEIR BEHALF
                sender_had_cs = True
            else:
                cs_items.append(cs_item)
            i += 1
            # TODO JAKIS LOCK
            self._critical_section_queue = PriorityQueue(number_of_other_hosts + 1)
        for cs_item in cs_items:
            self._critical_section_queue.put(cs_item)

        with self._hosts_acks_lock:
            self._hosts_acks.pop(sender_id)
        with self._hosts_replies_lock:
            self._hosts_replies.pop(sender_id)
        with self._sleep_set_lock:
            self._sleep_set.discard(sender_id)

        # CZY MOZE NAJPIERW TO OBSLUZYC A POTEM RELEASE? BO WTEDYB BUG
        if sender_had_cs:
            self._handle_release_msg(sender_id)

        self._send(MessageTypes.END_COMMUNICATION, sender_id)

        self.subscriber_socket.disconnect(f'tcp://{sender_ip}:{sender_port}')

    def _request(self):
        logging.debug(f'Host[{self.host_id}] requesting the critical section.')
        self._broadcast(MessageTypes.REQUEST)
        with self._lamport_lock:
            self._critical_section_queue.put((self._lamport_clock, self.host_id))

    def _reply(self, sender_id: int, sender_clock: int):
        logging.debug(f'Host[{self.host_id}] replying to host[{sender_id}].')
        self._critical_section_queue.put((sender_clock, sender_id))
        self._send(MessageTypes.REPLY, sender_id)

    def _release(self):
        logging.debug(f'Host[{self.host_id}] releasing the critical section.')
        # FUTURE UPGRADE IDEA -> INCLUDE self.shared_variables in the msg, so that others can sync up the data
        _ = self._critical_section_queue.get()
        with self._in_critical_section_lock:
            self._in_critical_section = False
        self._broadcast(MessageTypes.RELEASE)
        with self._should_signal_lock:
            if self._should_signal > 0:
                for _ in range(0, self._should_signal):
                    self._send_signal()
                self._should_signal = 0

    def _handle_sleep_msg(self, sender_id: int):
        logging.debug(f'Host[{self.host_id}] handling (SLEEP) from host[{sender_id}].')
        with self._sleep_set_lock:
            self._sleep_set.add(sender_id)

    def _handle_wake_msg(self, receiver_id: int):
        with self._sleep_set_lock:
            self._sleep_set.remove(receiver_id)
        if receiver_id == self.host_id:
            with self._asleep_lock:
                self._asleep = False
                self._asleep_lock.notifyAll()

    def begin_synchronized(self):
        # TODO: begin and end could return exit statuses.
        with self._instance_dead_lock:
            if self._instance_dead:
                logging.info(f'Host[{self.host_id}] is dead, cannot synchronize.')  # FIXME CZY HOST ID JESZCZE ISTNIEJE!!!
                return
        logging.debug(f'Host[{self.host_id}] is trying to enter the critical-section.')
        synchronization_phase = True
        while synchronization_phase:
            with self._synchronization_phase_lock:
                if not self._synchronization_phase:
                    synchronization_phase = False
            if not synchronization_phase:
                self._request()
                with self._in_critical_section_lock:
                    if self._in_critical_section:
                        logging.info(
                            f'Host[{self.host_id}] already in critical section.')
                        return
                    while not self._in_critical_section:
                        self._in_critical_section_lock.wait()
            else:
                # SHOULD NOT BE POSSIBLE TO RUN THIS WITHOUT PRE-SYNC ALREADY DONE
                logging.info(f'Host[{self.host_id}] has to wait, inter host synchronization in progress.')

    def end_synchronized(self):
        with self._instance_dead_lock:
            if self._instance_dead:
                logging.info(
                    f'Host[{self.host_id}] is dead, cannot end synchronization.')  # FIXME CZY HOST ID JESZCZE ISTNIEJE!!!
                return
        with self._in_critical_section_lock:
            if self._in_critical_section:
                self._in_critical_section = False
                self._release()
                logging.debug(f'Host[{self.host_id}] exited the critical section.')
            else:
                logging.info(f'Host[{self.host_id}] does no have the critical section. Cannot exit a monitor without entering it.')
        logging.debug(f'Host[{self.host_id}] exited monitor.')

    def _send_signal(self):
        # WAKES UP A HOST BUT ONLY AFTER SENDING RELEASE
        with self._sleep_set_lock:
            if len(self._sleep_set) != 0:
                # DONT SEND WAKE UP SIGNAL TO YOURSELF
                with self._other_host_ids_lock:
                    host_id_to_wakeup = random.choice(list(self._sleep_set.intersection(set(self.other_host_ids))))
                    self._sleep_set.remove(host_id_to_wakeup)
                    self._send(MessageTypes.WAKE, host_id_to_wakeup)
                logging.debug(f'Host[{self.host_id}] sent a wake-up signal to host[{host_id_to_wakeup}].')
            else:
                logging.debug(f'Host[{self.host_id}]\'s sleep queue is empty. No signaling has been done.')

    def signal(self):
        with self._instance_dead_lock:
            if self._instance_dead:
                logging.info(
                    f'Host[{self.host_id}] is dead, cannot signal.')
                return
        with self._in_critical_section_lock:
            if self._in_critical_section:
                with self._should_signal_lock:
                    self._should_signal += 1
            else:
                logging.debug(f'Host[{self.host_id}] not in critical section. No signaling has been done.')

    def signal_all(self):
        with self._instance_dead_lock:
            if self._instance_dead:
                logging.info(
                    f'Host[{self.host_id}] is dead, cannot signal_all.')
                return
        with self._in_critical_section_lock:
            if self._in_critical_section:
                with self._should_signal_lock:
                    # TODO LOCK NA LICZBE HOSTOW??
                    with self._sleep_set_lock:
                        with self._other_host_ids_lock:
                            self._should_signal = len(self._sleep_set.intersection(set(self.other_host_ids)))
            else:
                logging.debug(f'Host[{self.host_id}] not in critical section. No signaling has been done.')

    def block(self):
        with self._instance_dead_lock:
            if self._instance_dead:
                logging.info(
                    f'Host[{self.host_id}] is dead, cannot block.')
                return
        should_sleep = False
        with self._in_critical_section_lock:
            if self._in_critical_section:
                should_sleep = True
        if should_sleep:
            logging.debug(f'Host[{self.host_id}] RUNNING -> BLOCKING.')
            # ADD TO SLEEP SET
            with self._sleep_set_lock:
                self._sleep_set.add(self.host_id)
            # SET SLEEP LOCKs
            with self._release_cond_lock:
                self._release_cond = True
            with self._asleep_lock:
                self._asleep = True
            # SEND SLEEP MSG
            self._broadcast(MessageTypes.SLEEP)
            # RELEASE CS
            with self._in_critical_section_lock:
                if self._in_critical_section:
                    self._in_critical_section = False
                    self._release()
            # BLOCK UNTIL RELEASE
            with self._release_cond_lock:
                while self._release_cond:
                    self._release_cond_lock.wait()
            # BLOCK UNTIL SIGNAL
            with self._asleep_lock:
                while self._asleep:
                    self._asleep_lock.wait()
            self._request()
            with self._in_critical_section_lock:
                while not self._in_critical_section:
                    self._in_critical_section_lock.wait()
        logging.debug(f'Host[{self.host_id}] BLOCKING -> RUNNING.')

    def stop_monitor(self):
        with self._instance_dead_lock:
            if self._instance_dead:
                logging.info(
                    f'Host[{self.host_id}] is already dead.')  # FIXME CZY HOST ID JESZCZE ISTNIEJE!!!
                return
        logging.info(f'Host[{self.host_id}] sent END_COMMUNICATION. Remove instance afterwards.')
        with self._instance_dead_lock:
            self._instance_dead = True
        self._send(MessageTypes.END_COMMUNICATION, DistributedMonitor.BROADCAST_MESSAGE)
        # STOP THE LISTENING THREAD
        # IT IS A DEAMON THREAD ANYWAY SO SHOULD STOP WITH THE MAIN THREAD
        with self._subscriber_loop_lock:
            self._subscriber_loop = False

    def __del__(self):
        # Send END_COMMUNICATION when the Garbage Collector dumps the instance.
        self.stop_monitor()

    # def __repr__(self):
    #     pass
    #
    # def __string__(self):
    #     pass

