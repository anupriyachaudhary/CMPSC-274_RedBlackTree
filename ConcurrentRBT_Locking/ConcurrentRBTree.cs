﻿using System;
using System.Collections.Generic;
using System.Threading;

namespace ConcurrentRedBlackTree
{
    public class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private RedBlackNode<TKey, TValue> _root = new RedBlackNode<TKey, TValue>();
        private object rootLock = new Object();

        public Tuple<TKey, TValue> GetData(TKey key)
        {
            var node = GetNode(key);
            return node == null ? null : new Tuple<TKey, TValue>(node.Key, node.Data);
        }

        public void Add(TKey key, TValue value)
        {
            New(key, value);
        }

        public bool Remove(TKey key)
        {
            try
            {
                Delete(GetNode(key));
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public int MaxDepth()
        {
            return MaxDepthInternal(_root);
        }

        public int Count()
        {
            return CountInternal(_root);
        }

        private static int CountInternal(RedBlackNode<TKey, TValue> treeNode)
        {
            if (treeNode.IsSentinel)
            {
                return 0;
            }

            return CountInternal(treeNode.Left) + CountInternal(treeNode.Right) + 1;
        }

        private RedBlackNode<TKey, TValue> GetNode(TKey key)
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

        private void New(TKey key, TValue data)
        {
            if (data == null)
            {
                throw new Exception();
            }

            // traverse tree - find where node belongs

            // create new node
            var newNode = new RedBlackNode<TKey, TValue>(key, data);

            (RedBlackNode<TKey, TValue> aLocked, bool releaseRoot) = Insert(newNode);

            var localArea = new List<RedBlackNode<TKey, TValue>>();

            OccupyLocalArea(newNode, localArea);
            BalanceTreeAfterInsert(newNode, localArea);

            localArea.ForEach(node => node?.ReleaseELock());
            if (releaseRoot) Monitor.Exit(rootLock);
            aLocked?.ReleaseALock();
        }

        private (RedBlackNode<TKey, TValue>, bool) Insert(RedBlackNode<TKey, TValue> newNode)
        {
            RedBlackNode<TKey, TValue> aLocked;
        
            RedBlackNode<TKey, TValue> lastBlack = null;

            Monitor.Enter(rootLock);
            RedBlackNode<TKey, TValue> workNode = _root;
            workNode.GetALock();

            if (workNode.IsSentinel)
            {
                // workNode is root
                _root = newNode;
                _root.GetALock();
                workNode.ReleaseALock();
                return (_root, true);
            }

            bool releaseRoot = true;
            
            aLocked = workNode;

            while (true)
            {
                if (workNode.IsSentinel)
                {
                    break;
                }

                // find Parent
                newNode.Parent = workNode;
                int result = newNode.Key.CompareTo(workNode.Key);
                if (result == 0)
                {
                    throw new Exception("duplicate key");
                }

                if (workNode.Color == RedBlackNodeType.Black && lastBlack != _root)
                {
                    
                    if (lastBlack != null)
                    {
                        lastBlack.GetALock();
                        aLocked.ReleaseALock();
                        if(releaseRoot) Monitor.Exit(rootLock);
                        aLocked = lastBlack;
                        releaseRoot = false;
                    }
                    lastBlack = workNode;
                    
                }
                else
                {
                    if (workNode.Color == RedBlackNodeType.Black)
                    {
                        lastBlack = workNode;
                    }
                    else
                    {
                        lastBlack = null;
                    }
                }

                if (result > 0)
                {
                    workNode = workNode.Right;
                }
                else
                {
                    workNode = workNode.Left;
                }
            }

            if (newNode.Key.CompareTo(newNode.Parent.Key) > 0)
                newNode.Parent.Right = newNode;
            else
                newNode.Parent.Left = newNode;

            return (aLocked, releaseRoot);
        }
        
        private static void OccupyLocalArea(RedBlackNode<TKey, TValue> node, List<RedBlackNode<TKey, TValue>> working)
        {
            if (node.Parent == null)
            {
                node.GetELock();
                working.Add(node);
                return;
            }
            var grandParent = node.Parent.Parent;
            if (grandParent == null)
            {
                node.Parent.GetELock();
                node.GetELock();
                working.Add(node.Parent);
                working.Add(node);
                return;
            }
            
            var uncle = grandParent.Left == node.Parent ? grandParent.Right : grandParent.Left;

            grandParent.GetELock();
            uncle.GetELock();
            node.Parent.GetELock();
            node.GetELock();
            
            working.Add(grandParent);
            working.Add(uncle);
            working.Add(node.Parent);
            working.Add(node);
        }

        private static void MoveLocalAreaUpward(RedBlackNode<TKey, TValue> node, List<RedBlackNode<TKey, TValue>> working)
        {
            if (node.Parent == null)
            {
                node.GetELock();
                working.Add(node);
                return;
            }
            var grandParent = node.Parent.Parent;
            if (grandParent == null)
            {
                node.Parent.GetELock();
                node.GetELock();
                working.Add(node.Parent);
                working.Add(node);
                return;
            }
            
            var uncle = grandParent.Left == node.Parent ? grandParent.Right : grandParent.Left;

            grandParent.GetELock();
            uncle.GetELock();
            node.Parent.GetELock();
            node.GetELock();

            working.Add(grandParent);
            working.Add(uncle);
            working.Add(node.Parent);
            working.Add(node);
        }

        private void BalanceTreeAfterInsert(RedBlackNode<TKey, TValue> insertedNode, List<RedBlackNode<TKey, TValue>> working)
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
                        if (insertedNode == _root || insertedNode.Parent.Color != RedBlackNodeType.Red) break;
                        MoveLocalAreaUpward(insertedNode, working);
                    }
                    else
                    {
                        if (insertedNode == insertedNode.Parent.Right)
                        {
                            insertedNode = insertedNode.Parent;
                            RotateLeft(insertedNode);
                        }
                        insertedNode.Parent.Color = RedBlackNodeType.Black;
                        insertedNode.Parent.Parent.Color = RedBlackNodeType.Red;
                        RotateRight(insertedNode.Parent.Parent);
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
                        if (insertedNode == _root || insertedNode.Parent.Color != RedBlackNodeType.Red) break;
                        MoveLocalAreaUpward(insertedNode, working);
                    }
                    else
                    {
                        if (insertedNode == insertedNode.Parent.Left)
                        {
                            insertedNode = insertedNode.Parent;
                            RotateRight(insertedNode);
                        }
                        insertedNode.Parent.Color = RedBlackNodeType.Black;
                        insertedNode.Parent.Parent.Color = RedBlackNodeType.Red;
                        RotateLeft(insertedNode.Parent.Parent);
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

            if (rotateNode.Parent != null)
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

            if (rotateNode.Parent != null)
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
                _root = workNode;
            }

            workNode.Left = rotateNode;
            if (!rotateNode.IsSentinel)
            {
                rotateNode.Parent = workNode;
            }
        }

        private void Delete(RedBlackNode<TKey, TValue> deleteNode)
        {
            // A node to be deleted will be: 
            //		1. a leaf with no children
            //		2. have one child
            //		3. have two children
            // If the deleted node is red, the red black properties still hold.
            // If the deleted node is black, the tree needs rebalancing

            // work node
            RedBlackNode<TKey, TValue> workNode;

            // find the replacement node (the successor to x) - the node one with 
            // at *most* one child. 
            if (deleteNode.Left.IsSentinel || deleteNode.Right.IsSentinel)
                // node has sentinel as a child
                workNode = deleteNode;
            else
            {
                // z has two children, find replacement node which will 
                // be the leftmost node greater than z
                // traverse right subtree
                workNode = deleteNode.Right;
                // to find next node in sequence
                while (!workNode.Left.IsSentinel)
                    workNode = workNode.Left;
            }

            // at this point, y contains the replacement node. it's content will be copied 
            // to the valules in the node to be deleted

            // x (y's only child) is the node that will be linked to y's old parent. 
            RedBlackNode<TKey, TValue> linkedNode = !workNode.Left.IsSentinel
                                                 ? workNode.Left
                                                 : workNode.Right;

            // replace x's parent with y's parent and
            // link x to proper subtree in parent
            // this removes y from the chain
            linkedNode.Parent = workNode.Parent;
            if (workNode.Parent != null)
                if (workNode == workNode.Parent.Left)
                    workNode.Parent.Left = linkedNode;
                else
                    workNode.Parent.Right = linkedNode;
            else
            {
                // make x the root node
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
                BalanceTreeAfterDelete(linkedNode);
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
    }
}
