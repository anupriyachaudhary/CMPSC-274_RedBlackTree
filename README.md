# CMPSC-274_RedBlackTree

For sequential implementation of RBT from CMPSC-274_RedBlackTree/SequentialRBT_LockFree/ directory run the following commands:
$ dotnet build
$ dotnet run



For concurrent implementation of RBT from CMPSC-274_RedBlackTree/ConcurrentRBT_LockFree/ directory run the following commands:
$ dotnet build
$ dotnet run


ISSUE to be resolved:
(1) Insertion of the root node is sequential. Rest of the insertions run concurrently.
    Have to figure out how to insert root sequentially as well.

    
NOTE: 
When I run the concurrent implementation it outputs:
Total nodes to insert = (let's say) x
Node count after insertion = x + 1
2nd issue is due to the issue one. Since I first add the root and then add x nodes using a concurrent implementation
