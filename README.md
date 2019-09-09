# CMPSC-274_RedBlackTree

With the proliferation of multi-core, multi-processor systems, concurrent data structures capable of supporting several processes are a growing need. Concurrency is most often managed through locks but such lock-based data structures are vulnerable to problems such as deadlock, synchronization overheads and scheduling anomalies. Non-blocking algorithms avoid drawbacks of locks by using hardware-supported synchronization primitives.
In this project, I have implemented a lock-free algorithm for concurrently accessing a red-black tree in an asynchronous shared memory system that supports search, insert and delete operations using compare-and-swap (CAS) instructions. This algorithm is built upon the sequential implementation of red-black tree with the addition of a "local area" concept and it performs the rebalancing of modified tree in a bottom-up fashion. Based on my implementation and experiment results, this variant of lock-free algorithm significantly outperforms the sequential red-black tree.

Link to project presentation slides:
https://docs.google.com/presentation/d/1p5axbXUQ7fa-ON-EYLz5Y7MdvoQQCLLfTR3dDWRUteM/edit?usp=sharing



For sequential implementation of RBT from CMPSC-274_RedBlackTree/SequentialRBT_LockFree/ directory run the following commands:

$ dotnet build

$ dotnet run




For concurrent implementation of RBT from CMPSC-274_RedBlackTree/ConcurrentRBT_LockFree/ directory run the following commands:

$ dotnet build

$ dotnet run




