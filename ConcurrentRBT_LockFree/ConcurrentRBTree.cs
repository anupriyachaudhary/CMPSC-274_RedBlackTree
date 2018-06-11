using System;

namespace ConcurrentRedBlackTree
{
    public class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private RedBlackNode<TKey, TValue> _root = new RedBlackNode<TKey, TValue>();

        public Tuple<TKey, TValue> GetData(TKey key)
        {
            var node = GetNode(key);
            return node == null ? null : new Tuple<TKey, TValue>(node.Key, node.Data);
        }

        public void Add(TKey key, TValue value)
        {
            New(key, value);
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

            Insert(newNode);

            var localArea = new RedBlackNode<TKey, TValue>[4];
            localArea[0] = newNode;
            localArea[1] = newNode.Parent;
            localArea[2] = newNode.Parent?.Parent;

            if (newNode.Parent?.Parent != null)
            {
                localArea[3] = newNode.Parent.Parent.Left == newNode.Parent
                    ? newNode.Parent.Parent.Right
                    : newNode.Parent.Parent.Left;
            }

            // restore red-black properties
            BalanceTreeAfterInsert(newNode, localArea);

            foreach (var node in localArea)
            {
                node?.FreeNodeAtomically();
            }
        }

        private void Insert(RedBlackNode<TKey, TValue> newNode)
        {
            while (true)
            {
                RedBlackNode<TKey, TValue> workNode = _root;

                var IsLocalAreaOccupied = false;
                var isLeft = false;

                while (true)
                {
                    if (workNode.IsSentinel)
                    {
                        IsLocalAreaOccupied = newNode.Parent == null || OccupyLocalArea(newNode, isLeft);
                        break;
                    }

                    // find Parent
                    newNode.Parent = workNode;
                    int result = newNode.Key.CompareTo(workNode.Key);
                    if (result == 0)
                    {
                        throw new Exception("duplicate key");
                    }

                    if (result > 0)
                    {
                        isLeft = false;
                        workNode = workNode.Right;
                    }
                    else
                    {
                        isLeft = true;
                        workNode = workNode.Left;
                    }
                }

                if (IsLocalAreaOccupied)
                {
                    break;
                }
            }

            // insert node into tree starting at parent's location
            if (newNode.Parent != null)
            {
                if (newNode.Key.CompareTo(newNode.Parent.Key) > 0)
                    newNode.Parent.Right = newNode;
                else
                    newNode.Parent.Left = newNode;
            }
            else
            {
                // first node added
                _root = newNode;
            }
        }

        private static bool OccupyLocalArea(RedBlackNode<TKey, TValue> node, bool isLeft)
        {
            // occupy the node to be inserted
            if (!node.OccupyNodeAtomically())
            {
                return false;
            }

            // if parent is head no need to setup the local area
            if (node.Parent == null)
            {
                return true;
            }

            // occupy parent node atomically
            if (!node.Parent.OccupyNodeAtomically())
            {
                node.FreeNodeAtomically();
                return false;
            }

            // check if parent still pointing to sentinel node
            if ((isLeft && !node.Parent.Left.IsSentinel) || (!isLeft && !node.Parent.Right.IsSentinel))
            {
                node.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                return false;
            }

            var grandParent = node.Parent.Parent;

            // if no parent we are done
            if (grandParent == null)
            {
                return true;
            }

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

            if (uncle.IsSentinel)
            {
                return true;
            }

            if (!uncle.OccupyNodeAtomically())
            {
                grandParent.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }

            //check if uncle is not changed
            if (uncle.Parent != grandParent)
            {
                uncle.FreeNodeAtomically();
                grandParent.FreeNodeAtomically();
                node.Parent.FreeNodeAtomically();
                node.FreeNodeAtomically();
                return false;
            }

            return true;
        }

        private static void MoveLocalAreaUpward(RedBlackNode<TKey, TValue> node, RedBlackNode<TKey, TValue>[] working)
        {
            RedBlackNode<TKey, TValue> newParent = null, newGrandParent = null, newUncle = null;

            while (true)
            {
                if (node.Parent == null)
                {
                    break;
                }

                newParent = node.Parent;

                // occupy parent node atomically
                if (!newParent.OccupyNodeAtomically())
                {
                    continue;
                }

                // check if parent still pointing to node
                if (newParent.Left != node && newParent.Right != node)
                {
                    newParent.FreeNodeAtomically();
                    continue;
                }

                newGrandParent = newParent.Parent;

                // if no parent we are done
                if (newGrandParent == null)
                {
                    break;
                }

                // occupy grandparent   
                if (!newGrandParent.OccupyNodeAtomically())
                {
                    newParent.FreeNodeAtomically();
                    continue;
                }

                // if grand parent changed before occupying, return false
                if (newGrandParent != newParent.Parent)
                {
                    // free grand parent
                    newGrandParent.FreeNodeAtomically();
                    newParent.FreeNodeAtomically();
                    continue;
                }

                newUncle = newGrandParent.Left == node.Parent ? newGrandParent.Right : newGrandParent.Left;

                if (newUncle.IsSentinel)
                {
                    break;
                }

                if (!newUncle.OccupyNodeAtomically())
                {
                    newGrandParent.FreeNodeAtomically();
                    newParent.FreeNodeAtomically();
                    continue;
                }

                //check if uncle is not changed
                if (newUncle.Parent != newGrandParent)
                {
                    newUncle.FreeNodeAtomically();
                    newGrandParent.FreeNodeAtomically();
                    newParent.FreeNodeAtomically();
                    continue;
                }

                break;
            }

            // free the locks
            working[0]?.FreeNodeAtomically();
            working[1]?.FreeNodeAtomically();
            working[3]?.FreeNodeAtomically();

            working[0] = working[2];
            working[1] = newParent;
            working[2] = newGrandParent;
            working[3] = newUncle;
        }

        private void BalanceTreeAfterInsert(RedBlackNode<TKey, TValue> insertedNode,
            RedBlackNode<TKey, TValue>[] working)
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
    }
}