using System;
using System.Threading;

namespace ConcurrentRedBlackTree
{
    public class RedBlackNode<TKey, TValue>
        where TValue : class
        where TKey : IComparable, IComparable<TKey>, IEquatable<TKey>
    {
        private Object pLock;
        private int pCount = 0;
        private Object aLock;
        private Object eLock;
        
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
            pLock = new Object();
            aLock = new Object();
            eLock = new Object();
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
            pLock = new Object();
            aLock = new Object();
            eLock = new Object();
        }

        public void GetALock()
        {
            Console.WriteLine("aLock");
            Monitor.Enter(aLock);
            Console.WriteLine("aLock(f)");
        }

        public void ReleaseALock()
        {
            Monitor.Exit(aLock);
            Console.WriteLine("aLock(r)");
        }

        public void GetPLock()
        {
            Console.WriteLine("pLock");
            Monitor.Enter(pLock);
            pCount++;
            Monitor.Exit(pLock);
            Console.WriteLine("pLock(f)");
        }
        
        public void ReleasePLock()
        {
            Monitor.Enter(pLock);
            pCount--;
            Monitor.Exit(pLock);
        }

        public void GetELock()
        {
            Console.WriteLine("eLock");
            while (true)
            {
                Monitor.Enter(pLock);
                if (pCount == 0)
                {
                    break;
                }
                Monitor.Exit(pLock);
                Thread.Sleep(1);
            }
            Monitor.Enter(aLock);
            Monitor.Enter(eLock);
            Console.WriteLine("eLock(f)");
        }

        public void ReleaseELock()
        {
            Monitor.Exit(pLock);
            Monitor.Exit(aLock);
            Monitor.Exit(eLock);
        }
    }
}
