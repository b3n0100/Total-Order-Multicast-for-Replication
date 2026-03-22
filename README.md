# Total-Order-Multicast-for-Replication
***327 Assignment***

***1. Why does replication need total ordering for conflicting operations?***

Replication needs total ordering because conflicting updates can produce different results if replicas apply them in different orders. For example, suppose one replica receives put(x, "11") and another concurrent update is append(x, "10"). If one replica applies put first and then append, it may end with "1110", while another replica that applies append first and then put could end with "11". Even though both replicas saw the same updates, the final state differs because the order was different. Total ordering fixes this by forcing every replica to apply the same updates in the same global order.

***2. What do Lamport clocks guarantee and what do they not guarantee?***

Lamport clocks guarantee a logical ordering of events in a distributed system. In particular, if one event causally happens before another, then its Lamport timestamp will be smaller. That makes Lamport clocks useful for reasoning about causality and for helping replicas agree on a consistent ordering. However, Lamport clocks do not represent real physical time, so a smaller timestamp does not mean the event actually happened earlier in wall-clock time. They also do not fully detect concurrency by themselves. Two concurrent events can still be given different timestamps even though neither caused the other. To get a total order, we add a deterministic tie-breaker such as replica ID.

***3. Your algorithm assumes reliable FIFO communication. What breaks if messages can be lost or delivered out of FIFO order?***

If messages can be lost, then some replicas may never receive an update or an ACK, so they may wait forever or apply a different set of operations than other replicas. That breaks consistency. If messages are delivered out of FIFO order, then a replica might see a later message from a sender before an earlier one. This breaks the assumption behind tracking progress with timestamps, because a replica may incorrectly think it is safe to deliver the head of the queue even though an earlier message from that sender is still in transit. As a result, replicas could deliver updates in different orders and diverge. Reliable FIFO communication is what makes the delivery condition safe.

***4. Where is the “coordination” happening in your implementation (middleware vs application logic)?***

The coordination is happening in the middleware layer, not in the application logic. The application only defines the operations, like put, append, or incr, and applies them once they are delivered. The middleware is responsible for assigning Lamport timestamps, broadcasting messages, sending ACKs, maintaining the holdback queue, tracking progress from every replica, and deciding when a message is safe to deliver. In other words, the middleware provides the total-order multicast service, while the application simply uses that service to update the replicated store consistently.
