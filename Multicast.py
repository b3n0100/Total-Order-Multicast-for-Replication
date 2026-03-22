
from __future__ import annotations

import argparse
import heapq
import json
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

Timestamp = Tuple[int, int]  # (lamport_clock, replica_id)


@dataclass(order=True)
class QueueEntry:
    sort_key: Tuple[int, int] = field(init=False, repr=False)
    ts: Timestamp
    update_id: str
    op: Tuple[Any, ...]
    origin_id: int

    def __post_init__(self) -> None:
        self.sort_key = self.ts


@dataclass(order=True)
class ScheduledMessage:
    deliver_at: int
    seq: int
    src: int = field(compare=False)
    dst: int = field(compare=False)
    kind: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)


class Replica:
    def __init__(self, replica_id: int, replica_count: int) -> None:
        self.replica_id = replica_id
        self.replica_count = replica_count
        self.clock = 0
        self.store: Dict[str, Any] = {}
        self.max_seen: Dict[int, int] = {i: 0 for i in range(replica_count)}
        self.holdback_heap: List[QueueEntry] = []
        self.pending_by_id: Dict[str, QueueEntry] = {}
        self.delivered_order: List[str] = []
        self.event_log: List[str] = []
        self.delivery_log: List[str] = []
        self.seen_broadcasts: set[str] = set()

    def _log(self, text: str) -> None:
        self.event_log.append(text)

    def local_client_update(self, update_id: str, op: Tuple[Any, ...], network: "Network") -> None:
        self.clock += 1
        ts = (self.clock, self.replica_id)
        entry = QueueEntry(ts=ts, update_id=update_id, op=op, origin_id=self.replica_id)
        self._insert_holdback(entry)
        self.max_seen[self.replica_id] = max(self.max_seen[self.replica_id], ts[0])
        self.seen_broadcasts.add(update_id)
        self._log(f"CLIENT update {update_id} created at R{self.replica_id} ts={ts} op={op}")

        payload = {
            "update_id": update_id,
            "op": list(op),
            "ts": list(ts),
            "origin_id": self.replica_id,
        }
        for dst in range(self.replica_count):
            network.send(self.replica_id, dst, "TOBCAST", payload)

    def receive(self, src: int, kind: str, payload: Dict[str, Any], network: "Network") -> None:
        if kind == "TOBCAST":
            self._on_tobcast(src, payload, network)
        elif kind == "ACK":
            self._on_ack(src, payload)
        else:
            raise ValueError(f"Unknown message kind: {kind}")
        self.try_deliver()

    def _on_tobcast(self, src: int, payload: Dict[str, Any], network: "Network") -> None:
        update_id = payload["update_id"]
        op = tuple(payload["op"])
        ts = tuple(payload["ts"])
        origin_id = payload["origin_id"]

        self.clock = max(self.clock, ts[0]) + 1
        self.max_seen[origin_id] = max(self.max_seen[origin_id], ts[0])

        if update_id not in self.pending_by_id and update_id not in self.delivered_order:
            entry = QueueEntry(ts=ts, update_id=update_id, op=op, origin_id=origin_id)
            self._insert_holdback(entry)
            self._log(f"RECV TOBCAST {update_id} from R{src} at R{self.replica_id} ts={ts} op={op}")
        else:
            self._log(f"RECV duplicate TOBCAST {update_id} from R{src} at R{self.replica_id}")

        ack_ts = (self.clock, self.replica_id)
        ack_payload = {
            "update_id": update_id,
            "for_ts": list(ts),
            "ack_ts": list(ack_ts),
            "ack_sender": self.replica_id,
        }
        self.max_seen[self.replica_id] = max(self.max_seen[self.replica_id], ack_ts[0])
        for dst in range(self.replica_count):
            network.send(self.replica_id, dst, "ACK", ack_payload)

    def _on_ack(self, src: int, payload: Dict[str, Any]) -> None:
        ack_ts = tuple(payload["ack_ts"])
        ack_sender = payload["ack_sender"]
        self.clock = max(self.clock, ack_ts[0]) + 1
        self.max_seen[ack_sender] = max(self.max_seen[ack_sender], ack_ts[0])
        self._log(
            f"RECV ACK for {payload['update_id']} from R{src} at R{self.replica_id} ack_ts={ack_ts}; max_seen={self.max_seen}"
        )

    def _insert_holdback(self, entry: QueueEntry) -> None:
        self.pending_by_id[entry.update_id] = entry
        heapq.heappush(self.holdback_heap, entry)

    def _head_safe(self, entry: QueueEntry) -> bool:
        threshold = entry.ts[0]
        return all(self.max_seen[k] > threshold for k in range(self.replica_count))

    def try_deliver(self) -> None:
        while self.holdback_heap:
            head = self.holdback_heap[0]
            if head.update_id not in self.pending_by_id:
                heapq.heappop(self.holdback_heap)
                continue
            if not self._head_safe(head):
                break
            heapq.heappop(self.holdback_heap)
            self.pending_by_id.pop(head.update_id, None)
            self._apply(head)

    def _apply(self, entry: QueueEntry) -> None:
        op = entry.op
        kind = op[0]
        if kind == "put":
            _, key, value = op
            self.store[key] = value
        elif kind == "append":
            _, key, suffix = op
            current = self.store.get(key, "")
            self.store[key] = f"{current}{suffix}"
        elif kind == "incr":
            _, key, amount = op
            current = self.store.get(key, 0)
            self.store[key] = current + amount
        else:
            raise ValueError(f"Unsupported op: {op}")

        self.delivered_order.append(entry.update_id)
        text = (
            f"DELIVER {entry.update_id} at R{self.replica_id} ts={entry.ts} op={entry.op} "
            f"store={json.dumps(self.store, sort_keys=True)}"
        )
        self.delivery_log.append(text)
        self._log(text)


class Network:
    def __init__(self, replica_count: int, rng: random.Random, min_delay: int = 1, max_delay: int = 4) -> None:
        self.replica_count = replica_count
        self.rng = rng
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.time = 0
        self.seq = 0
        self.queue: List[ScheduledMessage] = []
        self.last_pair_delivery: Dict[Tuple[int, int], int] = {}

    def send(self, src: int, dst: int, kind: str, payload: Dict[str, Any]) -> None:
        delay = self.rng.randint(self.min_delay, self.max_delay)
        proposed = self.time + delay
        pair = (src, dst)
        deliver_at = max(proposed, self.last_pair_delivery.get(pair, 0) + 1)
        self.last_pair_delivery[pair] = deliver_at
        self.seq += 1
        msg = ScheduledMessage(deliver_at, self.seq, src, dst, kind, payload)
        heapq.heappush(self.queue, msg)

    def empty(self) -> bool:
        return not self.queue

    def step(self, replicas: List[Replica]) -> bool:
        if not self.queue:
            return False
        next_time = self.queue[0].deliver_at
        self.time = next_time

        batch: List[ScheduledMessage] = []
        while self.queue and self.queue[0].deliver_at == next_time:
            batch.append(heapq.heappop(self.queue))

        self.rng.shuffle(batch)
        batch.sort(key=lambda m: (m.src, m.seq))
        for msg in batch:
            replicas[msg.dst].receive(msg.src, msg.kind, msg.payload, self)
        return True


class Simulation:
    def __init__(self, replica_count: int, seed: int, min_delay: int = 1, max_delay: int = 4) -> None:
        self.replica_count = replica_count
        self.seed = seed
        self.rng = random.Random(seed)
        self.network = Network(replica_count, self.rng, min_delay, max_delay)
        self.replicas = [Replica(i, replica_count) for i in range(replica_count)]
        self.update_counter = 0

    def submit_update(self, target_replica: int, op: Tuple[Any, ...]) -> str:
        update_id = f"u{self.update_counter:03d}"
        self.update_counter += 1
        self.replicas[target_replica].local_client_update(update_id, op, self.network)
        return update_id

    def run(self) -> None:
        while self.network.step(self.replicas):
            pass
        for replica in self.replicas:
            replica.try_deliver()

    def assert_consistency(self) -> None:
        baseline_state = self.replicas[0].store
        baseline_order = self.replicas[0].delivered_order
        for replica in self.replicas[1:]:
            if replica.store != baseline_state:
                raise AssertionError(
                    f"Replica R{replica.replica_id} diverged state. {replica.store} != {baseline_state}"
                )
            if replica.delivered_order != baseline_order:
                raise AssertionError(
                    f"Replica R{replica.replica_id} diverged order. {replica.delivered_order} != {baseline_order}"
                )

    def summary(self) -> Dict[str, Any]:
        return {
            "seed": self.seed,
            "replica_count": self.replica_count,
            "final_state": self.replicas[0].store,
            "delivered_order": self.replicas[0].delivered_order,
        }

    def render_log(self, title: str) -> str:
        lines: List[str] = [f"=== {title} ===", f"seed={self.seed}", ""]
        for replica in self.replicas:
            lines.append(f"--- Replica R{replica.replica_id} event log ---")
            lines.extend(replica.event_log)
            lines.append("")
        lines.append("=== Final consistency check ===")
        lines.append(json.dumps(self.summary(), indent=2, sort_keys=True))
        return "\n".join(lines)


def scenario_conflicting(seed: int = 7) -> Tuple[Simulation, str]:
    sim = Simulation(replica_count=3, seed=seed)
    sim.submit_update(0, ("put", "x", "A"))
    sim.submit_update(1, ("append", "x", "B"))
    sim.submit_update(2, ("append", "x", "C"))
    sim.run()
    sim.assert_consistency()
    return sim, "conflicting_updates"


def scenario_high_contention(seed: int = 11, updates: int = 30) -> Tuple[Simulation, str]:
    sim = Simulation(replica_count=4, seed=seed)
    for i in range(updates):
        target = i % sim.replica_count
        sim.submit_update(target, ("incr", "acct", 1))
    sim.run()
    sim.assert_consistency()
    return sim, "high_contention"


def scenario_non_conflicting(seed: int = 19) -> Tuple[Simulation, str]:
    sim = Simulation(replica_count=5, seed=seed)
    ops = [
        (0, ("put", "a", 10)),
        (1, ("put", "b", 20)),
        (2, ("put", "c", 30)),
        (3, ("append", "msg", "hi")),
        (4, ("incr", "counter", 5)),
        (0, ("append", "msg", "!")),
    ]
    for target, op in ops:
        sim.submit_update(target, op)
    sim.run()
    sim.assert_consistency()
    return sim, "non_conflicting"


def run_named_scenario(name: str, seed: Optional[int] = None) -> Tuple[Simulation, str]:
    if name == "conflicting":
        return scenario_conflicting(seed if seed is not None else 7)
    if name == "contention":
        return scenario_high_contention(seed if seed is not None else 11)
    if name == "nonconflicting":
        return scenario_non_conflicting(seed if seed is not None else 19)
    raise ValueError(f"Unknown scenario: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Lamport-clock total-order multicast simulator")
    parser.add_argument(
        "--scenario",
        choices=["conflicting", "contention", "nonconflicting", "all"],
        default="all",
        help="Scenario to run",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional override seed")
    parser.add_argument("--log-dir", default="logs", help="Directory where logs will be written")
    args = parser.parse_args()

    import os

    os.makedirs(args.log_dir, exist_ok=True)
    scenario_names = [args.scenario] if args.scenario != "all" else ["conflicting", "contention", "nonconflicting"]

    for scenario_name in scenario_names:
        sim, label = run_named_scenario(scenario_name, args.seed)
        log_text = sim.render_log(label)
        path = os.path.join(args.log_dir, f"{label}.log")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(log_text)
        print(f"Wrote {path}")
        print(json.dumps(sim.summary(), indent=2, sort_keys=True))
        print()


if __name__ == "__main__":
    main()
