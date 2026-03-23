from __future__ import annotations

import heapq
import itertools
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

Timestamp = Tuple[int, int]  # (lamport_clock, replica_id)


# ----------------------------
# Part A1: Message definitions
# ----------------------------

@dataclass(frozen=True)
class Operation:
    kind: str
    key: str
    value: Any

    def __str__(self) -> str:
        return f"{self.kind}({self.key}, {self.value})"


@dataclass(frozen=True)
class TOBCAST:
    update_id: str
    op: Operation
    ts: Timestamp
    sender_id: int


@dataclass(frozen=True)
class ACK:
    update_id: str
    ts: Timestamp
    sender_id: int


# ----------------------------
# Part A2: Replica state
# ----------------------------

@dataclass
class HoldbackEntry:
    update_id: str
    op: Operation
    ts: Timestamp
    sender_id: int

    def key(self):
        return self.ts  # already includes deterministic tie-break (replica_id)


class Replica:
    def __init__(self, replica_id: int, n_replicas: int):
        self.id = replica_id
        self.n = n_replicas
        self.clock = 0

        # holdback keyed by update_id, ordering derived from ts
        self.holdback: Dict[str, HoldbackEntry] = {}

        # largest timestamp seen from each replica via TOBCAST or ACK
        self.max_seen: Dict[int, Timestamp] = {
            rid: (0, rid) for rid in range(n_replicas)
        }

        self.store: Dict[str, Any] = {}
        self.delivered: List[str] = []
        self.debug_log: List[str] = []

    def log(self, msg: str):
        self.debug_log.append(f"[R{self.id} C={self.clock}] {msg}")

    def _tick(self):
        self.clock += 1

    def _recv_clock_merge(self, incoming_ts: Timestamp):
        self.clock = max(self.clock, incoming_ts[0]) + 1

    def _update_max_seen(self, rid: int, ts: Timestamp):
        if ts > self.max_seen[rid]:
            self.max_seen[rid] = ts

    def on_client_update(self, update_id: str, op: Operation) -> TOBCAST:
        self._tick()
        ts = (self.clock, self.id)
        msg = TOBCAST(update_id=update_id, op=op, ts=ts, sender_id=self.id)
        self.log(f"CLIENT -> create TOBCAST {update_id} {op} ts={ts}")
        # local receive of own TOBCAST is handled by the network multicast
        return msg

    def on_receive_tobcast(self, msg: TOBCAST) -> ACK:
        self._recv_clock_merge(msg.ts)
        self.log(f"recv TOBCAST {msg.update_id} {msg.op} ts={msg.ts} from R{msg.sender_id}")

        if msg.update_id not in self.holdback:
            self.holdback[msg.update_id] = HoldbackEntry(
                update_id=msg.update_id,
                op=msg.op,
                ts=msg.ts,
                sender_id=msg.sender_id,
            )

        self._update_max_seen(msg.sender_id, msg.ts)

        # Send progress marker / ACK
        self._tick()
        ack_ts = (self.clock, self.id)
        ack = ACK(update_id=msg.update_id, ts=ack_ts, sender_id=self.id)
        self.log(f"send ACK for {msg.update_id} ts={ack_ts}")
        self.try_deliver()
        return ack

    def on_receive_ack(self, ack: ACK):
        self._recv_clock_merge(ack.ts)
        self._update_max_seen(ack.sender_id, ack.ts)
        self.log(f"recv ACK for {ack.update_id} ts={ack.ts} from R{ack.sender_id}")
        self.try_deliver()

    def sorted_holdback(self) -> List[HoldbackEntry]:
        return sorted(self.holdback.values(), key=lambda e: e.ts)

    def can_deliver_head(self) -> Optional[HoldbackEntry]:
        if not self.holdback:
            return None
        head = self.sorted_holdback()[0]
        if all(self.max_seen[rid] > head.ts for rid in range(self.n)):
            return head
        return None

    def try_deliver(self):
        while True:
            head = self.can_deliver_head()
            if head is None:
                return
            self.apply(head)
            del self.holdback[head.update_id]
            self.delivered.append(head.update_id)
            self.log(f"DELIVER {head.update_id} {head.op} ts={head.ts}")

    def apply(self, entry: HoldbackEntry):
        op = entry.op
        if op.kind == "put":
            self.store[op.key] = op.value
        elif op.kind == "append":
            cur = self.store.get(op.key, "")
            self.store[op.key] = str(cur) + str(op.value)
        elif op.kind == "incr":
            cur = self.store.get(op.key, 0)
            self.store[op.key] = cur + int(op.value)
        else:
            raise ValueError(f"Unknown op kind: {op.kind}")


# ----------------------------
# Part A3: FIFO network simulator
# ----------------------------

@dataclass(order=True)
class ScheduledEvent:
    deliver_time: float
    seq: int
    src: int = field(compare=False)
    dst: int = field(compare=False)
    payload: Any = field(compare=False)


class FIFONetwork:
    """
    Preserves FIFO per sender->receiver by ensuring each next message
    on that channel gets a delivery time >= previous delivery time.
    """

    def __init__(self, rng: random.Random, min_delay=1.0, max_delay=5.0):
        self.rng = rng
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.now = 0.0
        self.seq_counter = itertools.count()
        self.pq: List[ScheduledEvent] = []
        self.last_channel_delivery: Dict[Tuple[int, int], float] = {}

    def send(self, src: int, dst: int, payload: Any):
        base = self.now + self.rng.uniform(self.min_delay, self.max_delay)
        ch = (src, dst)
        deliver_time = max(base, self.last_channel_delivery.get(ch, 0.0) + 1e-6)
        self.last_channel_delivery[ch] = deliver_time
        heapq.heappush(
            self.pq,
            ScheduledEvent(
                deliver_time=deliver_time,
                seq=next(self.seq_counter),
                src=src,
                dst=dst,
                payload=payload,
            ),
        )

    def multicast(self, src: int, destinations: List[int], payload: Any):
        for dst in destinations:
            self.send(src, dst, payload)

    def run(self, replicas: Dict[int, Replica]):
        while self.pq:
            ev = heapq.heappop(self.pq)
            self.now = ev.deliver_time
            replica = replicas[ev.dst]

            if isinstance(ev.payload, TOBCAST):
                ack = replica.on_receive_tobcast(ev.payload)
                # ACK is multicast to everyone
                self.multicast(replica.id, list(replicas.keys()), ack)

            elif isinstance(ev.payload, ACK):
                replica.on_receive_ack(ev.payload)

            else:
                raise ValueError("Unknown payload type")


# ----------------------------
# Part B1: Experiment harness
# ----------------------------

class Simulator:
    def __init__(self, n_replicas: int, seed: int = 0):
        self.rng = random.Random(seed)
        self.replicas = {i: Replica(i, n_replicas) for i in range(n_replicas)}
        self.net = FIFONetwork(self.rng)

    def submit_update(self, target_replica: int, update_id: str, op: Operation):
        tob = self.replicas[target_replica].on_client_update(update_id, op)
        self.net.multicast(target_replica, list(self.replicas.keys()), tob)

    def run(self):
        self.net.run(self.replicas)

    def final_states(self):
        return {rid: dict(rep.store) for rid, rep in self.replicas.items()}

    def delivered_sequences(self):
        return {rid: list(rep.delivered) for rid, rep in self.replicas.items()}

    def check_correctness(self):
        states = list(self.final_states().values())
        seqs = list(self.delivered_sequences().values())
        same_state = all(s == states[0] for s in states)
        same_seq = all(s == seqs[0] for s in seqs)
        return same_state, same_seq

    def print_summary(self, title: str):
        print("=" * 80)
        print(title)
        print("-" * 80)
        same_state, same_seq = self.check_correctness()
        print("Identical final state:", same_state)
        print("Identical delivered sequence:", same_seq)
        print("Final state:", self.final_states()[0])
        print("Delivered order:", self.delivered_sequences()[0])
        print()

    def print_logs(self, per_replica_limit: Optional[int] = None):
        for rid, rep in self.replicas.items():
            print(f"--- Replica {rid} log ---")
            logs = rep.debug_log if per_replica_limit is None else rep.debug_log[:per_replica_limit]
            for line in logs:
                print(line)
            print()


# ----------------------------
# Part B2: Required experiments
# ----------------------------

def experiment_conflicting_updates():
    sim = Simulator(n_replicas=3, seed=7)
    sim.submit_update(0, "u1", Operation("put", "x", "A"))
    sim.submit_update(1, "u2", Operation("append", "x", "B"))
    sim.submit_update(2, "u3", Operation("append", "x", "C"))
    sim.run()
    sim.print_summary("Experiment 1: Concurrent conflicting updates")
    return sim


def experiment_high_contention(num_updates=30):
    sim = Simulator(n_replicas=5, seed=11)
    for i in range(num_updates):
        target = i % 5
        sim.submit_update(target, f"u{i}", Operation("incr", "acct", 1))
    sim.run()
    sim.print_summary(f"Experiment 2: High contention ({num_updates} increments)")
    return sim


def experiment_non_conflicting():
    sim = Simulator(n_replicas=4, seed=21)
    updates = [
        (0, "u1", Operation("put", "a", 10)),
        (1, "u2", Operation("put", "b", 20)),
        (2, "u3", Operation("append", "c", "X")),
        (3, "u4", Operation("append", "d", "Y")),
        (0, "u5", Operation("incr", "e", 3)),
        (2, "u6", Operation("incr", "f", 4)),
    ]
    for target, uid, op in updates:
        sim.submit_update(target, uid, op)
    sim.run()
    sim.print_summary("Experiment 3: Non-conflicting updates")
    return sim


if __name__ == "__main__":
    sim1 = experiment_conflicting_updates()
    sim2 = experiment_high_contention(30)
    sim3 = experiment_non_conflicting()

    # Uncomment to inspect detailed logs:
    #sim1.print_logs()
    #sim2.print_logs(per_replica_limit=40)
    #sim3.print_logs()