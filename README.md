# Total-Order Multicast for Replication

## How to Run

```bash
python Multicast.py
```

## Description

We implemented a total-order multicast system using Lamport clocks.

* Each replica timestamps updates
* Messages are broadcast using `TOBCAST`
* Replicas send `ACK`s to track progress
* Updates are stored in a **holdback queue**
* Messages are only delivered when it is safe

This guarantees all replicas apply updates in the same order.

## Experiments

The program runs 3 tests:

1. Conflicting updates (same key)
2. High contention (many updates)
3. Non-conflicting updates (different keys)

For each test, it checks:

* All replicas have the same final state
* All replicas have the same delivery order

## Logs

To see detailed logs (TOBCAST, ACK, DELIVER), uncomment:

```python
sim1.print_logs()
sim2.print_logs(per_replica_limit=40)
sim3.print_logs()
```

## Diagram

```
Clients
|         \         (clients can send to any replica)
v          v
+----+ +----+ +----+ +----+
 | R1 |  | R2 |   | R3 |   | R4 |
+----+ +----+ +----+ +----+
   \           |          /          |
     \------|------/--------|
      total-order multicast
(TOBCAST + ACK, holdback queues, deliver only when safe)
```
