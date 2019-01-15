using System;

namespace ConcurrentRedBlackTree
{
    public class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private enum DeleteOccupyState
        {
            Success = 0,
            Failure = 1,

            Exit = 2
        }

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
                return Delete(key);
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

                var isLocalAreaOccupied = false;
                var isLeft = false;

                while (true)
                {
                    if (workNode.IsSentinel)
                    {
                        isLocalAreaOccupied = newNode.Parent == null || OccupyLocalAreaForInsert(newNode, isLeft);
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

                if (isLocalAreaOccupied)
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

        private static bool OccupyLocalAreaForInsert(RedBlackNode<TKey, TValue> node, bool isLeft)
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

        private static void MoveLocalAreaUpwardForInsert(RedBlackNode<TKey, TValue> node, RedBlackNode<TKey, TValue>[] working)
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
                        MoveLocalAreaUpwardForInsert(insertedNode, working);
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
                        MoveLocalAreaUpwardForInsert(insertedNode, working);
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

        private bool Delete(TKey key)
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

        private DeleteOccupyState OccupyLocalAreaForDelete(RedBlackNode<TKey, TValue> deleteNode, RedBlackNode<TKey, TValue> workNode, RedBlackNode<TKey, TValue>[] localArea)
        {
            // occupy the node to be deleted
            if (!deleteNode.OccupyNodeAtomically())
            {
                return DeleteOccupyState.Failure;
            }

            // check if correct node is locked
            if(GetNode(deleteNode.Key) == null)
            {
                deleteNode.FreeNodeAtomically();
                return DeleteOccupyState.Exit;
            }

            // if delete node and work node are different than lock work node as well
            if(deleteNode != workNode)
            {
                if(!workNode.OccupyNodeAtomically())
                {
                    deleteNode.FreeNodeAtomically();
                    return DeleteOccupyState.Failure;
                }
                var node = deleteNode.Right;
                while (!node.Left.IsSentinel)
                {
                    node = node.Left;
                }

                if(node != workNode)
                {
                    deleteNode.FreeNodeAtomically();
                    workNode.FreeNodeAtomically();
                    return DeleteOccupyState.Failure;
                }
                localArea[0] = deleteNode;
            }

            RedBlackNode<TKey, TValue> child;
            // occupy the child of work node
            if(!workNode.Left.IsSentinel)
            {
                child = workNode.Left;
            }
            else
            {
                child = workNode.Right;
            }

            if(!child.OccupyNodeAtomically())
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                return DeleteOccupyState.Failure;
            }

            localArea[1] = child;
            
            // if parent is head no need to setup the local area
            if (workNode.Parent == null)
            {
                return DeleteOccupyState.Failure;
            }

            bool isLeft = workNode.Parent.Left == workNode;

            // occupy parent node atomically
            if (!workNode.Parent.OccupyNodeAtomically())
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            if ((isLeft && workNode.Parent.Left != workNode) || (!isLeft && workNode.Parent.Right != workNode))
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            localArea[2] = workNode.Parent;

            var sibling = isLeft ? workNode.Parent.Right : workNode.Parent.Left;

            if (!sibling.OccupyNodeAtomically())
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            //check if sibling is not changed
            if ((isLeft && workNode.Parent.Right != sibling) || (!isLeft && workNode.Parent.Left != sibling))
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                sibling.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            localArea[3] = sibling;

            var siblingLeftChild = sibling.Left;
            // lock left child of sibling
            if(!siblingLeftChild.OccupyNodeAtomically())
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                sibling.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            if(sibling.Left != siblingLeftChild)
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                sibling.FreeNodeAtomically();
                siblingLeftChild.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            localArea[4] = siblingLeftChild;

            var siblingRightChild = sibling.Right;
            // lock left child of sibling
            if(!siblingRightChild.OccupyNodeAtomically())
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                sibling.FreeNodeAtomically();
                siblingLeftChild.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            if(sibling.Right != siblingRightChild)
            {
                deleteNode.FreeNodeAtomically();
                if(deleteNode != workNode)
                {
                    workNode.FreeNodeAtomically();
                }
                child.FreeNodeAtomically();
                workNode.Parent.FreeNodeAtomically();
                sibling.FreeNodeAtomically();
                siblingLeftChild.FreeNodeAtomically();
                siblingRightChild.FreeNodeAtomically();
                return DeleteOccupyState.Failure;
            }

            localArea[5] = siblingRightChild;

            return DeleteOccupyState.Success;
        }

        private static void MoveLocalAreaUpwardForDelete(RedBlackNode<TKey, TValue> node, RedBlackNode<TKey, TValue>[] working)
        {
            RedBlackNode<TKey, TValue> newParent = null, newSibling = null, newsiblingLeftChild = null, newSiblingRightChild = null;

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

                bool isNodeLeft = node.Parent.Left == node;

                // check if parent still pointing to node
                if ((isLeft && newParent.Left != node) || (!isLeft && newParent.Right != node))
                {
                    newParent.FreeNodeAtomically();
                    continue;
                }

                newSibling = isLeft ? newParent.Right : newParent.Left;

                // if no sibling we are done
                if (newSibling == null)
                {
                    break;
                }

                // occupy sibling   
                if (!newSibling.OccupyNodeAtomically())
                {
                    newParent.FreeNodeAtomically();
                    continue;
                }

                // if sibling changed before occupying, return false
                if ((isLeft && newParent.Right != newSibling) || (!isLeft && newParent.Left != newSibling))
                {
                    // free grand parent
                    newSibling.FreeNodeAtomically();
                    newParent.FreeNodeAtomically();
                    continue;
                }

                newSiblingLeftChild = newSibling.Left;;

                if (newSiblingLeftChild.IsSentinel)
                {
                    break;
                }

                if (!newSiblingLeftChild.OccupyNodeAtomically())
                {
                    newSibling.FreeNodeAtomically();
                    newParent.FreeNodeAtomically();
                    continue;
                }

                //check if left sibling is not changed
                if (newSibling.Left != newSiblingLeftChild)
                {
                    newParent.FreeNodeAtomically();
                    newSibling.FreeNodeAtomically();
                    newSiblingLeftChild.FreeNodeAtomically();
                    continue;
                }

                newSiblingRightChild = newSibling.Right;;

                if (newSiblingRightChild.IsSentinel)
                {
                    break;
                }

                if (!newSiblingRightChild.OccupyNodeAtomically())
                {
                    newSibling.FreeNodeAtomically();
                    newParent.FreeNodeAtomically();
                    continue;
                }

                //check if left sibling is not changed
                if (newSibling.Right != newSiblingRightChild)
                {
                    newParent.FreeNodeAtomically();
                    newSibling.FreeNodeAtomically();
                    newSiblingRightChild.FreeNodeAtomically();
                    continue;
                }

                break;
            }

            // free the locks
            working[1]?.FreeNodeAtomically();
            working[3]?.FreeNodeAtomically();
            working[4]?.FreeNodeAtomically();
            working[5]?.FreeNodeAtomically();

            working[1] = working[2];
            working[2] = newParent;
            working[3] = newSibling;
            working[4] = newSiblingLeftChild;
            working[5] = newSiblingRightChild;
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