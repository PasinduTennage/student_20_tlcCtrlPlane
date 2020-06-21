# An Asynchronous Control Plane for CRUX

CRUX is a locality preserving replication system for key value stores that provides availability and strong
consistency guarantees of a key value store service in the face of network partitions and other faults [1]. The
control plane for CRUX is the algorithm which initiates the CRUX using a set of geographically distributed
nodes and handles subsequent node churn and node movements (mobile nodes).

The current control plane of CRUX [2] is implemented using synchronous protocols. Synchronous protocols
make the underlying assumption that the set of nodes have synchronized clocks, which does not hold in
practice due to clock skew and drift. Hence the current control plane algorithm fails to operate correctly in
the absence of synchronized clocks.

To address this problem with synchronized CRUX control plane ,in this project, we propose a novel control
plane algorithm for CRUX which is fully asynchronous. Our asynchronous CRUX control plane algorithm
provides support for initiating a new CRUX instance, joining new nodes to an existing CRUX system and
handling node leavings (can be both planned and unplanned), in an asynchronized system. We use Threshold
logical clock (TLC) [3] as the asynchronous logical clock algorithm in order to produce a synchronized time
abstraction to the control plane running on top of a fully asynchronous system. We also employ the QSC
protocol [3] as the consensus protocol which runs on top of the abstraction provided by TLC.


## How to run

Clone the repo and execute `go build && ./simulation service.toml > qsc.txt  2>&1` in the simulation directory.

## License

Double-licensed under a GNU/AGPL 3.0 and a commercial license. If you want to have more information,
contact us at dedis@epfl.ch.

## References

[1] C. Basescu, M. Nowlan, K. Nikitin, J. Faleiro and B. Ford, "Crux: Locality-Preserving Distributed
Services", arXiv.org, 2018. [Online]. Available: https://arxiv.org/abs/1405.0637. [Accessed: 19- Jun- 2020].


[2] Pannatier. A, “A Control Plane in Time and Space for Locality-Preserving Blockchains”, Master Thesis,
École Polytechnique Fédérale de Lausanne, 2020


[3] Ford. B, Jovanovic. P, Syta. E, “Que Sera Consensus: Simple Asynchronous Agreement with Private
Coins and Threshold Logical Clocks”, arXiv.org, 2020. [Online]. Available:
https://arxiv.org/pdf/2003.02291.pdf . [Accessed: 19- Jun- 2020].

