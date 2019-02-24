using System;
using System.Threading;

namespace ConcurrentRedBlackTree
{
    public class RedBlackNode<TKey, TValue>
        where TValue : class
        where TKey : IComparable, IComparable<TKey>, IEquatable<TKey>
    {
        private int flag;

        public Guid Marker {get; set; }

        public TValue Data { get; set; }

        public TKey Key { get; set; }

        public RedBlackNodeType Color { get; set; }

        public bool IsSentinel { get; }

        public RedBlackNode<TKey, TValue> Left { get; set; }

        public RedBlackNode<TKey, TValue> Right { get; set; }

        public RedBlackNode<TKey, TValue> Parent { get; set; }

        public RedBlackNode()
        {
            Key = default(TKey);
            Data = default(TValue);
            Marker = Guid.Empty;
            Color = RedBlackNodeType.Black;
            IsSentinel = true;
            Left = null;
            Right = null;
            Parent = null;
        }

        public RedBlackNode(TKey key, TValue data)
            : this()
        {
            Key = key;
            Data = data;
            Marker = Guid.Empty;
            Color = RedBlackNodeType.Red;
            IsSentinel = false;
            Left = new RedBlackNode<TKey, TValue>();
            Right = new RedBlackNode<TKey, TValue>();
            Parent = null;
        }

        public void FreeNodeAtomically()
        {
            Interlocked.CompareExchange(ref flag, 0, 1);
        }

        public bool OccupyNodeAtomically()
        {
            return 0 == Interlocked.CompareExchange(ref flag, 1, 0);
        }
    }
}