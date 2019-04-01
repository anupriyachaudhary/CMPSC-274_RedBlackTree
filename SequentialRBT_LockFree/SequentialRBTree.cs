using System;

namespace SequentialRBTree
{
    public class SequentialRBTree<TKey, TValue>
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
            var treeNode = _root;

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

            /* compute the depth of each subtree */
            var lDepth = MaxDepthInternal(node.Left);
            var rDepth = MaxDepthInternal(node.Right);

            /* use the larger one */
            if (lDepth > rDepth)
            {
                return lDepth + 1;
            }

            return rDepth + 1;
        }

        private void New(TKey key, TValue data)
        {
            if (data == null)
            {
                throw new Exception();
            }

            // create new node
            var newNode = new RedBlackNode<TKey, TValue>(key, data);
            var workNode = _root;

            while (!workNode.IsSentinel)
            {
                // find Parent
                newNode.Parent = workNode;
                int result = key.CompareTo(workNode.Key);
                if (result == 0)
                {
                    throw new Exception();
                }

                workNode = result > 0
                    ? workNode.Right
                    : workNode.Left;
            }

            // insert node into tree starting at parent's location
            if (newNode.Parent != null)
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
            else
            {
                _root = newNode;
            }

            // restore red-black properties
            BalanceTreeAfterInsert(newNode);
        }

        private void BalanceTreeAfterInsert(RedBlackNode<TKey, TValue> insertedNode)
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
                        insertedNode = insertedNode.Parent.Parent;
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

            if (!workNode.Right.IsSentinel) workNode.Right.Parent = rotateNode;

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
            RedBlackNode<TKey, TValue> workNode;

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

            RedBlackNode<TKey, TValue> linkedNode = !workNode.Left.IsSentinel ? workNode.Left : workNode.Right;

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

            if (workNode != deleteNode)
            {
                deleteNode.Key = workNode.Key;
                deleteNode.Data = workNode.Data;
            }

            if (workNode.Color == RedBlackNodeType.Black)
            {
                BalanceTreeAfterDelete(linkedNode);
            }
        }

        private void BalanceTreeAfterDelete(RedBlackNode<TKey, TValue> linkedNode)
        {
            while (linkedNode != _root && linkedNode.Color == RedBlackNodeType.Black)
            {
                RedBlackNode<TKey, TValue> workNode;
                if (linkedNode == linkedNode.Parent.Left)
                {
                    workNode = linkedNode.Parent.Right;
                    if (workNode.Color == RedBlackNodeType.Red)
                    {
                        linkedNode.Parent.Color = RedBlackNodeType.Red;
                        workNode.Color = RedBlackNodeType.Black;
                        RotateLeft(linkedNode.Parent);
                        workNode = linkedNode.Parent.Right;
                    }
                    if (workNode.Left.Color == RedBlackNodeType.Black &&
                        workNode.Right.Color == RedBlackNodeType.Black)
                    {
                        workNode.Color = RedBlackNodeType.Red;
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
                        workNode.Color = linkedNode.Parent.Color;
                        linkedNode.Parent.Color = RedBlackNodeType.Black;
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