using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ConcurrentRedBlackTree
{
    public partial class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private const int DummyNodesCount = 8;

        private RedBlackNode<TKey, TValue> _dummy;

        private RedBlackNode<TKey, TValue> _root;

        private ConcurrentDictionary<Guid, List<MoveUpStruct<TKey, TValue>>> moveUpStructDict
            = new ConcurrentDictionary<Guid, List<MoveUpStruct<TKey, TValue>>>();

        public ConcurrentRBTree()
        {
            _dummy = new RedBlackNode<TKey, TValue>(default(TKey), default(TValue));
            _dummy.Color = RedBlackNodeType.Black;

            var current = _dummy;
            for(var i = 0; i < (DummyNodesCount - 1); i++)
            {
                current.Parent = new RedBlackNode<TKey, TValue>(default(TKey), default(TValue));
                current.Parent.Color = RedBlackNodeType.Black;
                current = current.Parent;
            }
        }

        public void Add(TKey key, TValue value)
        {
            Guid pid = Guid.NewGuid();
            try
            {
                New(key, value, pid);
            }
            catch (Exception)
            {
                return;
            }
            finally
            {
                moveUpStructDict.TryRemove(pid, out var _);
            }
        }

        private void New(TKey key, TValue data, Guid pid)
        {
            if (data == null)
            {
                throw new Exception();
            }

            // create new node
            var newNode = new RedBlackNode<TKey, TValue>(key, data);

            var localArea = new RedBlackNode<TKey, TValue>[4];

            Insert(newNode, localArea, pid);

            if(_root.Left.IsSentinel && _root.Right.IsSentinel)
            {
                _root.Color = RedBlackNodeType.Black;
                _dummy.FreeNodeAtomically();
                _dummy.Left.FreeNodeAtomically();
                return;
            }

            // restore red-black properties
            BalanceTreeAfterInsert(newNode, localArea, pid);

            // Release markers of local area
            var intentionMarkers = new RedBlackNode<TKey, TValue>[4];

            var top = localArea[2];

            while(true)
            {
                if(GetFlagsForMarkers(top, pid, intentionMarkers, null))
                {
                    break;
                }
            }
            foreach (var node in intentionMarkers)
            {
                // if(node.Marker != pid)
                // {
                //     Console.WriteLine($"{node.Marker}, {pid}, {Thread.CurrentThread.Name}");
                // }
                node.Marker  = Guid.Empty;
            }
            ReleaseFlagsAfterFailure(intentionMarkers, pid);

            foreach (var node in localArea)
            {
                node?.FreeNodeAtomically();
            }
        }

        private void Insert(RedBlackNode<TKey, TValue> newNode, RedBlackNode<TKey, TValue>[] localArea, Guid pid)
        {
            while (true)
            {
                if(_root == null)
                {
                    if(!_dummy.OccupyNodeAtomically())
                    {
                        continue;
                    }
                    if(!_dummy.Left.IsSentinel)
                    {
                        _dummy.FreeNodeAtomically();
                        continue;
                    }
                    newNode.OccupyNodeAtomically();

                    // first node added
                    newNode.Parent = _dummy;
                    _dummy.Left = newNode;
                    _root = newNode;
                    break;
                }

                RedBlackNode<TKey, TValue> workNode = _root, nextNode = _root;
                if(!workNode.OccupyNodeAtomically())
                {
                    continue;
                }
                var isLocalAreaOccupied = false;

                while (true)
                {
                    newNode.Parent = workNode;
                    int result = newNode.Key.CompareTo(workNode.Key);
                    if (result == 0)
                    {
                        throw new Exception("duplicate key");
                    }

                    if (result > 0)
                    {
                        nextNode = workNode.Right;
                    }
                    else
                    {
                        nextNode = workNode.Left;
                    }

                    if(nextNode.IsSentinel)
                    {
                        isLocalAreaOccupied = OccupyLocalAreaForInsert(newNode, localArea, pid);
                        break;
                    }

                    if(!nextNode.OccupyNodeAtomically())
                    {
                        workNode.FreeNodeAtomically();
                        break;
                    }

                    if (nextNode.Parent != workNode)
                    {
                        workNode.FreeNodeAtomically();
                        nextNode.FreeNodeAtomically();
                        break;
                    }

                    workNode.FreeNodeAtomically();
                    workNode = nextNode;
                }

                if(isLocalAreaOccupied)
                {
                    break;
                }
            }

            // insert node into tree starting at parent's location
            if (newNode != _root)
            {
                if (newNode.Key.CompareTo(newNode.Parent.Key) > 0)
                {
                    newNode.Parent.Right = newNode;
                }
                else
                {
                    newNode.Parent.Left = newNode;
                }
            }
        }

        private bool OccupyLocalAreaForInsert(RedBlackNode<TKey, TValue> node, RedBlackNode<TKey, TValue>[] localArea, Guid pid)
        {
            // occupy the node to be inserted
            if (!node.OccupyNodeAtomically())
            {
                node.Parent.FreeNodeAtomically();
                return false;
            }

            var grandParent = node.Parent.Parent;

            // occupy grandparent   
            if (!grandParent.OccupyNodeAtomically())
            {
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }

            // if grand parent changed before occupying, return false
            if (grandParent != node.Parent.Parent)
            {
                // free grand parent
                grandParent.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }

            var uncle = grandParent.Left == node.Parent ? grandParent.Right : grandParent.Left;

            if (!uncle.OccupyNodeAtomically())
            {
                grandParent.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }

            //check if uncle is not changed
            var temp_uncle = (grandParent.Left == node.Parent ? grandParent.Right : grandParent.Left);
            if (uncle != temp_uncle)
            {
                uncle.FreeNodeAtomically();
                grandParent.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }

            localArea[0] = node;
            localArea[1] = node.Parent;
            localArea[2] = grandParent;
            localArea[3] = uncle;

            if(!GetFlagsAndMarkersAbove(grandParent, localArea, pid, 0))
            {
                uncle.FreeNodeAtomically();
                grandParent.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }
            return true;
        }

        private void BalanceTreeAfterInsert(RedBlackNode<TKey, TValue> insertedNode,
            RedBlackNode<TKey, TValue>[] localArea, Guid pid)
        {
            // maintain red-black tree properties after adding newNode
            while (insertedNode != _root && insertedNode.Parent.Color == RedBlackNodeType.Red)
            {
                RedBlackNode<TKey, TValue> workNode;
                if (insertedNode.Parent == insertedNode.Parent.Parent.Left)
                {
                    workNode = insertedNode.Parent.Parent.Right;
                    if (workNode != null && workNode.Color == RedBlackNodeType.Red)
                    {
                        insertedNode.Parent.Color = RedBlackNodeType.Black;
                        workNode.Color = RedBlackNodeType.Black;

                        insertedNode.Parent.Parent.Color = RedBlackNodeType.Red;

                        // continue loop with grandparent
                        insertedNode = insertedNode.Parent.Parent;
                        MoveLocalAreaUpwardForInsert(insertedNode, localArea, pid);
                    }
                    else
                    {
                        if (insertedNode == insertedNode.Parent.Right)
                        {
                            insertedNode = insertedNode.Parent;
                            RotateLeft(insertedNode);
                            FixUpForInsertCase3(localArea, pid);
                        }

                        insertedNode.Parent.Color = RedBlackNodeType.Black;
                        insertedNode.Parent.Parent.Color = RedBlackNodeType.Red;

                        RedBlackNode<TKey, TValue> gp = null;
                        while(true)
                        {
                            gp = insertedNode.Parent.Parent.Parent;
                            if(!gp.OccupyNodeAtomically())
                            {
                                continue;
                            }

                            if(insertedNode.Parent.Parent.Parent != gp)
                            {
                                gp.FreeNodeAtomically();
                                continue;
                            }

                            break;
                        }

                        RotateRight(insertedNode.Parent.Parent);
                        gp.FreeNodeAtomically();
                        FixUpForInsertCase4(localArea, pid);
                    }
                }
                else
                {
                    workNode = insertedNode.Parent.Parent.Left;
                    if (workNode != null && workNode.Color == RedBlackNodeType.Red)
                    {
                        insertedNode.Parent.Color = RedBlackNodeType.Black;
                        workNode.Color = RedBlackNodeType.Black;
                        insertedNode.Parent.Parent.Color = RedBlackNodeType.Red;

                        // continue loop with grandparent
                        insertedNode = insertedNode.Parent.Parent;
                        MoveLocalAreaUpwardForInsert(insertedNode, localArea, pid);
                    }
                    else
                    {
                        if (insertedNode == insertedNode.Parent.Left)
                        {
                            insertedNode = insertedNode.Parent;
                            RotateRight(insertedNode);
                            FixUpForInsertCase3(localArea, pid);
                        }

                        insertedNode.Parent.Color = RedBlackNodeType.Black;
                        insertedNode.Parent.Parent.Color = RedBlackNodeType.Red;

                        RedBlackNode<TKey, TValue> gp = null;
                        while(true)
                        {
                            gp = insertedNode.Parent.Parent.Parent;
                            if(!gp.OccupyNodeAtomically())
                            {
                                continue;
                            }

                            if(insertedNode.Parent.Parent.Parent != gp)
                            {
                                gp.FreeNodeAtomically();
                                continue;
                            }

                            break;
                        }
                        RotateLeft(insertedNode.Parent.Parent);
                        gp.FreeNodeAtomically();
                        FixUpForInsertCase4(localArea, pid);
                    }
                }
            }

            _root.Color = RedBlackNodeType.Black;
        }

        private void MoveLocalAreaUpwardForInsert(RedBlackNode<TKey, TValue> node, RedBlackNode<TKey, TValue>[] localArea, Guid pid)
        {
            RedBlackNode<TKey, TValue> newParent = node.Parent, newGrandParent = node.Parent.Parent, newUncle = null;

            while (true)
            {
                if(GetFlagsAndMarkersAbove(node, localArea, pid, 2))
                {
                    break;
                }
            }

            while(true)
            {
                newUncle = newGrandParent.Left == node.Parent ? newGrandParent.Right : newGrandParent.Left;

                if (!IsIn(newUncle, pid))
                {
                    if(newUncle.OccupyNodeAtomically())
                    {
                        //check if uncle is not changed
                        var temp_uncle = newGrandParent.Left == node.Parent ? newGrandParent.Right : newGrandParent.Left;
                        if (newUncle != temp_uncle)
                        {
                            newUncle.FreeNodeAtomically();
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                else
                {
                    break;
                }
            }

            // release flag on old local area
            List<RedBlackNode<TKey, TValue>> nodesToRelease = new List<RedBlackNode<TKey, TValue>>();
            nodesToRelease.Add(localArea[0]);
            nodesToRelease.Add(localArea[1]);
            nodesToRelease.Add(localArea[3]);
            ReleaseFlagsAfterSuccess(nodesToRelease, pid);

            localArea[0] = node;
            localArea[1] = newParent;
            localArea[2] = newGrandParent;
            localArea[3] = newUncle;
        }

        private void FixUpForInsertCase3(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            //  correct relocated intention markers for other processes
            if (localArea[1].Marker != Guid.Empty 
                && localArea[0].Marker == localArea[1].Marker)
            {
                localArea[2].Marker = localArea[0].Marker;
                localArea[1].Marker = Guid.Empty; 
            }

            var parentOtherChild = (localArea[0] == localArea[1].Right) ? localArea[1].Left : localArea[1].Right;
            if (localArea[1].Marker != Guid.Empty 
                && parentOtherChild.Marker == localArea[1].Marker
                && localArea[2].Marker == localArea[1].Marker)
            {
                localArea[2].Marker = Guid.Empty;
                localArea[0].Marker = localArea[1].Marker; 
            }

            // Correct Local area
            var newParent = localArea[0];
            
            localArea[0] = localArea[1];
            localArea[1] = newParent;
        }

        private void FixUpForInsertCase4(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            var movedChild = (localArea[0] == localArea[1].Right) ? localArea[1].Left : localArea[1].Right;

            //  correct relocated intention markers for other processes
            if (localArea[1].Marker != Guid.Empty 
                && movedChild.Marker == localArea[1].Marker
                && localArea[2].Marker == Guid.Empty)
            {
                localArea[2].Marker = localArea[1].Marker;
                localArea[1].Marker = Guid.Empty; 
            }

            // Correct Local area
            var temp = localArea[1];
            
            localArea[1] = localArea[2];
            localArea[2] = temp;
        }

    }
}