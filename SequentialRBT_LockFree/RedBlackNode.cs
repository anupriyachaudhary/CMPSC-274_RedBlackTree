﻿using System;

namespace SequentialRBTree
{
    public class RedBlackNode<TKey, TValue>
        where TValue : class
        where TKey : IComparable, IComparable<TKey>, IEquatable<TKey>
    {
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
            Color = RedBlackNodeType.Red;
            IsSentinel = false;
            Left = new RedBlackNode<TKey, TValue>();
            Right = new RedBlackNode<TKey, TValue>();
            Parent = null;
        }
    }
}
