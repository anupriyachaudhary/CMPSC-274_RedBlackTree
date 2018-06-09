# CMPSC-274_RedBlackTree

For sequential implementation of RBT from CMPSC-274_RedBlackTree/SequentialRBT_LockFree/ directory run the following commands:
$ dotnet build
$ dotnet run



For concurrent implementation of RBT from CMPSC-274_RedBlackTree/ConcurrentRBT_LockFree/ directory run the following commands:
$ dotnet build
$ dotnet run


ISSUES to be resolved:
(1) Insertion of the root node is sequential. Rest of the insertions run concurrently.
    Have to figure out how to insert root sequentially as well.
    Since since root added seperately, run the command outputs:
    Total nodes to insert = (let's say) x
    Node count after insertion = x + 1
