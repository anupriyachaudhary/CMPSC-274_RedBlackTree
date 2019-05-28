using System;
using System.Collections.Generic;
using System.Threading;
using System.Collections.Concurrent;
using System.Linq;

namespace ConcurrentRedBlackTree
{
    public partial class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private RedBlackNode<TKey, TValue> _dummy;

        private RedBlackNode<TKey, TValue> _root;

        public ConcurrentRBTree()
        {
            _dummy = new RedBlackNode<TKey, TValue>(default(TKey), default(TValue));
            _dummy.Color = RedBlackNodeType.Black;

            var current = _dummy;
            for(var i = 0; i < 5; i++)
            {
                current.Parent = new RedBlackNode<TKey, TValue>(default(TKey), default(TValue));
                current.Parent.Color = RedBlackNodeType.Black;
                current = current.Parent;
            }
        }

        public RedBlackNode<TKey, TValue> GetNode(TKey key)
        {
            // begin at root
            RedBlackNode<TKey, TValue> treeNode = _root;

            // traverse tree until node is found
            while (!treeNode.IsSentinel)
            {
                var result = key.CompareTo(treeNode.Key);
                if (result == 0)
                {
                    return treeNode;
                }

                treeNode = result < 0
                    ? treeNode.Left
                    : treeNode.Right;
            }

            return null;
        }

        public Tuple<TKey, TValue> GetData(TKey key)
        {
            var node = GetNode(key);
            return node == null ? null : new Tuple<TKey, TValue>(node.Key, node.Data);
        }

        public int MaxDepth()
        {
            return MaxDepthInternal(_root);
        }

        public int Count()
        {
            return CountInternal(_root);
        }

        private int CountInternal(RedBlackNode<TKey, TValue> treeNode)
        {
            if (treeNode.IsSentinel)
            {
                return 0;
            }

            return CountInternal(treeNode.Left) + CountInternal(treeNode.Right) + 1;
        }

        private static int MaxDepthInternal(RedBlackNode<TKey, TValue> node)
        {
            if (node.IsSentinel)
            {
                return 0;
            }
            else
            {
                /* compute the depth of each subtree */
                var lDepth = MaxDepthInternal(node.Left);
                var rDepth = MaxDepthInternal(node.Right);

                /* use the larger one */
                if (lDepth > rDepth)
                {
                    return lDepth + 1;
                }
                else
                {
                    return rDepth + 1;
                }
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

            // traverse tree - find where node belongs

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

            RedBlackNode<TKey, TValue> top = localArea[2];

            while(true)
            {
                if(GetFlagsForMarkers(top, pid, intentionMarkers, localArea[0]))
                {
                    break;
                }
            }
            foreach (var node in intentionMarkers)
            {
                node.Marker  = Guid.Empty;
            }
            ReleaseFlags(pid, false, intentionMarkers.ToList());

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

        private void MoveLocalAreaUpwardForInsert(RedBlackNode<TKey, TValue> node, RedBlackNode<TKey, TValue>[] working, Guid pid)
        {
            RedBlackNode<TKey, TValue> newParent = node.Parent, newGrandParent = node.Parent.Parent, newUncle = null;

            while (true)
            {
                if(GetFlagsAndMarkersAbove(node, working, pid, 2))
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
            nodesToRelease.Add(working[0]);
            nodesToRelease.Add(working[1]);
            nodesToRelease.Add(working[3]);
            ReleaseFlags(pid, true, nodesToRelease);

            working[0] = node;
            working[1] = newParent;
            working[2] = newGrandParent;
            working[3] = newUncle;
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


        private void BalanceTreeAfterInsert(RedBlackNode<TKey, TValue> insertedNode,
            RedBlackNode<TKey, TValue>[] working, Guid pid)
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
                        MoveLocalAreaUpwardForInsert(insertedNode, working, pid);
                    }
                    else
                    {
                        if (insertedNode == insertedNode.Parent.Right)
                        {
                            insertedNode = insertedNode.Parent;
                            RotateLeft(insertedNode);
                            FixUpForInsertCase3(working, pid);
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
                        FixUpForInsertCase4(working, pid);
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
                        MoveLocalAreaUpwardForInsert(insertedNode, working, pid);
                    }
                    else
                    {
                        if (insertedNode == insertedNode.Parent.Left)
                        {
                            insertedNode = insertedNode.Parent;
                            RotateRight(insertedNode);
                            FixUpForInsertCase3(working, pid);
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
                        FixUpForInsertCase4(working, pid);
                    }
                }
            }

            _root.Color = RedBlackNodeType.Black;
        }
       
        private void RotateRight(RedBlackNode<TKey, TValue> rotateNode)
        {
            var workNode = rotateNode.Left;

            rotateNode.Left = workNode.Right;

            if (!workNode.Right.IsSentinel)
            {
                workNode.Right.Parent = rotateNode;
            }

            if (!workNode.IsSentinel)
            {
                workNode.Parent = rotateNode.Parent;
            }

            if (rotateNode.Parent != _dummy)
            {
                if (rotateNode == rotateNode.Parent.Right)
                {
                    rotateNode.Parent.Right = workNode;
                }
                else
                {
                    rotateNode.Parent.Left = workNode;
                }
            }
            else
            {
                workNode.Parent = _dummy;
                _dummy.Left = workNode;
                _root = workNode;
            }

            workNode.Right = rotateNode;
            if (!rotateNode.IsSentinel)
            {
                rotateNode.Parent = workNode;
            }
        }

        private void RotateLeft(RedBlackNode<TKey, TValue> rotateNode)
        {
            var workNode = rotateNode.Right;

            rotateNode.Right = workNode.Left;
            if (!workNode.Left.IsSentinel)
            {
                workNode.Left.Parent = rotateNode;
            }

            if (!workNode.IsSentinel)
            {
                workNode.Parent = rotateNode.Parent;
            }

            if (rotateNode.Parent != _dummy)
            {
                if (rotateNode == rotateNode.Parent.Left)
                {
                    rotateNode.Parent.Left = workNode;
                }
                else
                {
                    rotateNode.Parent.Right = workNode;
                }
            }
            else
            {
                workNode.Parent = _dummy;
                _dummy.Left = workNode;
                _root = workNode;
            }

            workNode.Left = rotateNode;
            if (!rotateNode.IsSentinel)
            {
                rotateNode.Parent = workNode;
            }
        }
    
        public bool isValidRBT(TKey nodesMaxKeyValue)
        {
            return isValidRBT(_root, default(TKey), nodesMaxKeyValue);
        }

        private bool isValidRBT(RedBlackNode<TKey, TValue> node, TKey low, TKey high)
        {
            if(node.IsSentinel)
                return true;

            bool isLow = false;
            bool isHigh = false;
            if(node.Key.CompareTo(low) > 0)
            {
                isLow = true;
            }
            if(node.Key.CompareTo(high) < 0)
            {
                isHigh = true;
            }
            if(isLow == false || isHigh == false)
            {
                Console.WriteLine($"RBT is invalid at {node.Key}");
            }
            return isLow && isHigh
                    && isValidRBT(node.Left, low, node.Key) 
                    && isValidRBT(node.Right, node.Key, high);
        }

        public void printLevelOrder()
        {
            int height = MaxDepth();
            int i; 
            for (i=1; i <= height; i++) 
            { 
                printGivenLevel(_root, i); 
                Console.WriteLine("\n"); 
            }
        }

        private void printGivenLevel(RedBlackNode<TKey, TValue> node, int level) 
        { 
            if (node.IsSentinel)
            {
                Console.Write("null ");
                return;
            }
            if (level == 1) 
                Console.Write($"{node.Key} "); 
            else if (level > 1) 
            { 
                printGivenLevel(node.Left, level-1); 
                printGivenLevel(node.Right, level-1); 
            } 
        }
    }
}