using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace ConcurrentRedBlackTree
{
    public static class ConcurrentRBTreeHelper
    {
        public static bool OccupyAndCheck<TKey, TValue>(
            Func<RedBlackNode<TKey, TValue>> assign,
            Func<bool> check = null,
            Action rollback = null)
            where TValue : class
            where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
        {
            // assign
            var node = assign.Invoke();

            if (check == null)
            {
                // try occupying
                if (!node.OccupyNodeAtomically())
                {
                    rollback?.Invoke();
                    return false;
                }
            }
            else
            {
                if (check.Invoke() && !node.OccupyNodeAtomically())
                {
                    rollback?.Invoke();
                    return false;
                }
            }

            //check
            if(node == assign.Invoke())
            {
                return true;
            }

            //release
            node.FreeNodeAtomically();
            return false;
        }
    }
}
