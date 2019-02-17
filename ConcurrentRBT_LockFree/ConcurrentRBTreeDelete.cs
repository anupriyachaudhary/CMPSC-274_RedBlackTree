using System;

namespace ConcurrentRedBlackTree
{
    public partial class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private static object Lock = new object();

        private enum DeleteOccupyState
        {
            Success = 0,
            Failure = 1,

            Exit = 2
        }

        private void UpdateMoveUpStructWithLock(Guid guid, MoveUpStruct<TKey, TValue> moveUpStruct)
        {
            lock (Lock)
            {
                moveUpStructDict[guid] = moveUpStruct;
            }
        }

        private MoveUpStruct<TKey, TValue> GetMoveUpStructWithLock(Guid guid)
        {
            lock (Lock)
            {
                return moveUpStructDict[guid];
            }
        }
       
        public bool Remove(TKey key)
        {
            Guid guid = Guid.NewGuid();
            moveUpStructDict.Add(guid, new MoveUpStruct<TKey, TValue>());
            try
            {
                return Delete(key, guid);
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                moveUpStructDict.Remove(guid);
            }
        }

        private bool Delete(TKey key, Guid guid)
        {
            RedBlackNode<TKey, TValue> workNode = null, linkedNode = null, deleteNode = null;
            var localArea = new RedBlackNode<TKey, TValue>[6];
            while (true)
            {
                deleteNode = GetNode(key);
                if(deleteNode == null)
                {
                    return false;
                }

                if (deleteNode.Left.IsSentinel || deleteNode.Right.IsSentinel)
                {
                    workNode = deleteNode;
                }
                else
                {                    
                    workNode = deleteNode.Right;
                    while (!workNode.Left.IsSentinel)
                    {
                        workNode = workNode.Left;
                    }
                }

                var status = OccupyLocalAreaForDelete(deleteNode, workNode, localArea);
                if(status == DeleteOccupyState.Success)
                {
                    break;
                }
                else if(status == DeleteOccupyState.Exit)
                {
                    return false;
                }
            }

            if(!workNode.Left.IsSentinel)
            {
                linkedNode = workNode.Left;
            }
            else
            {
                linkedNode = workNode.Right;
            }

            linkedNode.Parent = workNode.Parent;
            if (workNode.Parent != null)
            {
                if (workNode == workNode.Parent.Left)
                {
                    workNode.Parent.Left = linkedNode;
                }
                else
                {
                    workNode.Parent.Right = linkedNode;
                }
            }
            else
            {
                _root = linkedNode;
            }

            // copy the values from y (the replacement node) to the node being deleted.
            // note: this effectively deletes the node. 
            if (workNode != deleteNode)
            {
                deleteNode.Key = workNode.Key;
                deleteNode.Data = workNode.Data;
            }

            if (workNode.Color == RedBlackNodeType.Black)
            {
                BalanceTreeAfterDelete(linkedNode);
            }

            foreach (var node in localArea)
            {
                node?.FreeNodeAtomically();
            }
            
            return true;
        }

        private void BalanceTreeAfterDelete(RedBlackNode<TKey, TValue> linkedNode)
        {
            // maintain Red-Black tree balance after deleting node
            while (linkedNode != _root && linkedNode.Color == RedBlackNodeType.Black)
            {
                RedBlackNode<TKey, TValue> workNode;
                // determine sub tree from parent
                if (linkedNode == linkedNode.Parent.Left)
                {
                    // y is x's sibling
                    workNode = linkedNode.Parent.Right;
                    if (workNode.Color == RedBlackNodeType.Red)
                    {
                        // x is black, y is red - make both black and rotate
                        linkedNode.Parent.Color = RedBlackNodeType.Red;
                        workNode.Color = RedBlackNodeType.Black;
                        RotateLeft(linkedNode.Parent);
                        workNode = linkedNode.Parent.Right;
                    }
                    if (workNode.Left.Color == RedBlackNodeType.Black &&
                        workNode.Right.Color == RedBlackNodeType.Black)
                    {
                        // children are both black
                        // change parent to red
                        workNode.Color = RedBlackNodeType.Red;
                        // move up the tree
                        linkedNode = linkedNode.Parent;
                    }
                    else
                    {
                        if (workNode.Right.Color == RedBlackNodeType.Black)
                        {
                            workNode.Left.Color = RedBlackNodeType.Black;
                            workNode.Color = RedBlackNodeType.Red;
                            RotateRight(workNode);
                            workNode = linkedNode.Parent.Right;
                        }
                        linkedNode.Parent.Color = RedBlackNodeType.Black;
                        workNode.Color = linkedNode.Parent.Color;
                        workNode.Right.Color = RedBlackNodeType.Black;
                        RotateLeft(linkedNode.Parent);
                        linkedNode = _root;
                    }
                }
                else
                {	// right subtree - same as code above with right and left swapped
                    workNode = linkedNode.Parent.Left;
                    if (workNode.Color == RedBlackNodeType.Red)
                    {
                        linkedNode.Parent.Color = RedBlackNodeType.Red;
                        workNode.Color = RedBlackNodeType.Black;
                        RotateRight(linkedNode.Parent);
                        workNode = linkedNode.Parent.Left;
                    }
                    if (workNode.Right.Color == RedBlackNodeType.Black &&
                        workNode.Left.Color == RedBlackNodeType.Black)
                    {
                        workNode.Color = RedBlackNodeType.Red;
                        linkedNode = linkedNode.Parent;
                    }
                    else
                    {
                        if (workNode.Left.Color == RedBlackNodeType.Black)
                        {
                            workNode.Right.Color = RedBlackNodeType.Black;
                            workNode.Color = RedBlackNodeType.Red;
                            RotateLeft(workNode);
                            workNode = linkedNode.Parent.Left;
                        }
                        workNode.Color = linkedNode.Parent.Color;
                        linkedNode.Parent.Color = RedBlackNodeType.Black;
                        workNode.Left.Color = RedBlackNodeType.Black;
                        RotateRight(linkedNode.Parent);
                        linkedNode = _root;
                    }
                }
            }
            linkedNode.Color = RedBlackNodeType.Black;
        }

        private bool OccupyLocalAreaForDelete(
            RedBlackNode<TKey, TValue> y,
            RedBlackNode<TKey, TValue> z,
            RedBlackNode<TKey, TValue>[] localArea)
        {
            var x = y.Left.IsSentinel ? y.Right : y.Left;

            // occupy the node to be deleted
            if (!x.OccupyNodeAtomically())
            {
                return false;
            }

            var yp = y.Parent;

            if(yp != z && !yp.OccupyNodeAtomically())
            {
                x.FreeNodeAtomically();
                return false;
            }

            // verify that parent is unchanged
            if(yp != y.Parent)
            {
                x.FreeNodeAtomically();
                if(yp != z)
                {
                    yp.FreeNodeAtomically();
                }
                return false;
            }

            var w = y == y.Parent.Left ? y.Parent.Right : y.Parent.Left;

            if(!w.OccupyNodeAtomically())
            {
                x.FreeNodeAtomically();
                if(yp != z)
                {
                    yp.FreeNodeAtomically();
                }
                return false;
            }

            if(!w.IsSentinel)
            {
                var wlc = w.Left;
                var wrc = w.Right;

                if(!wlc.OccupyNodeAtomically())
                {
                    x.FreeNodeAtomically();
                    w.FreeNodeAtomically();
                    if(yp != z)
                    {
                        yp.FreeNodeAtomically();
                    }
                    return false;
                }

                if(!wrc.OccupyNodeAtomically())
                {
                    x.FreeNodeAtomically();
                    w.FreeNodeAtomically();
                    wlc.FreeNodeAtomically();
                    if(yp != z)
                    {
                        yp.FreeNodeAtomically();
                    }
                    return false;
                }
            }
        }

        private void ApplyMoveUpRule(
            RedBlackNode<TKey, TValue> x,
            RedBlackNode<TKey, TValue> w)
        {
            var case1 = w.Marker == w.Parent.Marker &&
                w.Marker == w.Right.Marker &&
                w.Marker != 0 &&
                w.Left.Marker != 0;

            var case2 = w.Marker == w.Right.Marker &&
                w.Marker != 0 &&
                w.Left.Marker != 0;

            var case3 = w.Marker == 0 &&
                w.Left.Marker != 0 &&
                w.Right.Marker != 0;
            if(case1 || case2 || case3)
            {
                
            }
            
        }
    }
}