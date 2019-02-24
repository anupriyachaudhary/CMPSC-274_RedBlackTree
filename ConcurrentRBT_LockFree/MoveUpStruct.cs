using System;
using System.Collections.Generic;

namespace ConcurrentRedBlackTree
{
    public class MoveUpStruct<TKey, TValue>
        where TValue : class
        where TKey : IComparable, IComparable<TKey>, IEquatable<TKey>
    {
        public MoveUpStruct()
        {
            PidToIgnore = Guid.Empty;
        }

        public List<RedBlackNode<TKey, TValue>> Nodes {get; set;}

        public Guid PidToIgnore {get; set;}

        public RedBlackNode<TKey, TValue> Gp {get; set;}
    }
}