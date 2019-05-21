using System;
using System.Collections.Generic;
using System.Linq;

namespace ConcurrentRedBlackTree
{
    public partial class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        public bool Remove(TKey key)
        {
            Guid pid = Guid.NewGuid();
            try
            {
                return Delete(key, pid);
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                moveUpStructDict.TryRemove(pid, out var _);
            }
        }
        
        private RedBlackNode<TKey, TValue> GetNodeForDelete(TKey key)
        {
            // begin at root
            RedBlackNode<TKey, TValue> treeNode = _root;

            // Hold a flag on root
            if(!treeNode.OccupyNodeAtomically())
            {
                return null;
            }
            // check if _root is locked
            if(treeNode != _root)
            {
                treeNode.FreeNodeAtomically();
                return null;
            }
            var previousNode = treeNode;

            // traverse tree until node is found
            while (!treeNode.IsSentinel)
            {
                var result = key.CompareTo(treeNode.Key);
                if (result == 0)
                {
                    return treeNode;
                }

                treeNode = result < 0 ? treeNode.Left : treeNode.Right;
                // Hold a flag on new treenode
                if(!treeNode.OccupyNodeAtomically())
                {
                    previousNode.FreeNodeAtomically();
                    return null;
                }
                // check if correct node is locked
                if(treeNode != (result < 0 ? previousNode.Left : previousNode.Right))
                {
                    previousNode.FreeNodeAtomically();
                    treeNode.FreeNodeAtomically();
                    return null;
                }
                previousNode.FreeNodeAtomically();
                previousNode = treeNode;
            }

            treeNode.FreeNodeAtomically();
            return null;
        }

        private bool Delete(TKey key, Guid pid)
        {
            RedBlackNode<TKey, TValue> y = null, z = null, x = null;
            var localArea = new RedBlackNode<TKey, TValue>[5];
            
            while (true)
            {
                // GetNode will return a locked node
                z = GetNodeForDelete(key);

                if(z == null)
                {
                    continue;
                }

                // Find key-order successor, locked y is returned
                y = FindSuccessor(z);               
                if(y == null)
                {
                    //Console.WriteLine("No successor");
                    z.FreeNodeAtomically();
                    continue;
                }
                //we  now hold a flag on y and z

                if(!SetupLocalAreaForDelete(y, localArea, pid, z))
                {
                    y.FreeNodeAtomically();
                    if(y != z)
                    {
                        z.FreeNodeAtomically();
                    }
                    continue;
                }
                else
                {
                    break;
                }      
            }

            x = y.Left.IsSentinel ? y.Right : y.Left;

            // unlink y from the tree
            x.Parent = y.Parent;
            if (y.Parent != null)
            {
                if (y == y.Parent.Left)
                {
                    y.Parent.Left = x;
                }
                else
                {
                    y.Parent.Right = x;
                }
            }
            else
            {
                _root = x;
            }

            // copy the values from y (the replacement node) to the node being deleted.
            // note: this effectively deletes the node. 
            if (y != z)
            {
                z.Key = y.Key;
                z.Data = y.Data;
                // Release z only when it is not in local area
                bool isZinLocalArea = false;
                foreach(var node in localArea)
                {
                    if(node == z)
                    {
                        isZinLocalArea = true;
                        break;
                    }
                }
                if(!isZinLocalArea)
                {
                    z.FreeNodeAtomically();
                }
                y.FreeNodeAtomically();  
            }

            if (y.Color == RedBlackNodeType.Black)
            {
                BalanceTreeAfterDelete(x, localArea, pid);
            }
            else
            {
                // Release markers of local area
                var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
                while(true)
                {
                    if(GetFlagsForMarkers(localArea[1], pid, intentionMarkers, z))
                    {
                        break;
                    }
                }
                foreach (var node in intentionMarkers)
                {
                    node.Marker  = Guid.Empty;
                }
                ReleaseFlagsAfterFailure(intentionMarkers.ToList(), pid);
                
                // Release flags of local area
                foreach (var node in localArea)
                {
                    node?.FreeNodeAtomically();
                }
            }

            return true;
        }

        private RedBlackNode<TKey, TValue> FindSuccessor(RedBlackNode<TKey, TValue> z)
        {
            if (z.Left.IsSentinel || z.Right.IsSentinel)
            {
                return z;
            }

            var nextNode = z.Right;
            // Hold a flag on root
            if(!nextNode.OccupyNodeAtomically())
            {
                return null;
            }
            // check if correct node is locked
            if(nextNode != z.Right)
            {
                nextNode.FreeNodeAtomically();
                return null;
            }
            
            var successor = nextNode;

            while (true)
            {
                nextNode = successor.Left;
                // Hold a flag on new treenode
                if(!nextNode.OccupyNodeAtomically())
                {
                    successor.FreeNodeAtomically();
                    return null;
                }
                // check if correct node is locked
                if(nextNode != successor.Left)
                {
                    successor.FreeNodeAtomically();
                    nextNode.FreeNodeAtomically();
                    return null;
                }
                if (nextNode.IsSentinel)
                {
                    nextNode.FreeNodeAtomically();
                    return successor;
                }
                successor.FreeNodeAtomically();
                successor = nextNode;
            }
        }

        private bool SetupLocalAreaForDelete(
            RedBlackNode<TKey, TValue> y,
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid,
            RedBlackNode<TKey, TValue> z = null)
        {
            var x = y.Left.IsSentinel ? y.Right : y.Left;

            // occupy the node which will replace y
            if (!x.OccupyNodeAtomically())
            {
                return false;
            }
            localArea[0] = x;

            var yp = y.Parent;
            bool isNotCheckZ = (z == null || yp != z);

            if(isNotCheckZ && !yp.OccupyNodeAtomically())
            {
                x.FreeNodeAtomically();
                return false;
            }

            // verify that parent is unchanged
            if(yp != y.Parent)
            {
                x.FreeNodeAtomically();
                if(isNotCheckZ)
                {
                    yp.FreeNodeAtomically();
                }
                return false;
            }
            localArea[1] = yp;

            var w = (y == yp.Left ? yp.Right : yp.Left);

            if(!w.OccupyNodeAtomically())
            {
                x.FreeNodeAtomically();
                if(isNotCheckZ)
                {
                    yp.FreeNodeAtomically();
                }
                return false;
            }
            
            // verify if sibling is changed
            var nw = (y == yp.Left ? yp.Right : yp.Left);
            if(w != nw)
            {
                x.FreeNodeAtomically();
                w.FreeNodeAtomically();
                if(isNotCheckZ)
                {
                    yp.FreeNodeAtomically();
                }
                return false;
            }
            localArea[2] = w;

            var wlc = w.Left;
            var wrc = w.Right;

            if(!w.IsSentinel)
            {
                if(!wlc.OccupyNodeAtomically())
                {
                    x.FreeNodeAtomically();
                    w.FreeNodeAtomically();
                    if(isNotCheckZ)
                    {
                        yp.FreeNodeAtomically();
                    }
                    return false;
                }

                // validate
                if(w.Left != wlc)
                {
                    x.FreeNodeAtomically();
                    w.FreeNodeAtomically();
                    wlc.FreeNodeAtomically();
                    if(isNotCheckZ)
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
                    if(isNotCheckZ)
                    {
                        yp.FreeNodeAtomically();
                    }
                    return false;
                }

                // validate
                if(w.Right != wrc)
                {
                    x.FreeNodeAtomically();
                    w.FreeNodeAtomically();
                    wlc.FreeNodeAtomically();
                    wrc.FreeNodeAtomically();
                    if(isNotCheckZ)
                    {
                        yp.FreeNodeAtomically();
                    }
                    return false;
                }

                if (w == yp.Right)
                {
                    localArea[3] = wlc;
                    localArea[4] = wrc;
                }
                else
                {
                    localArea[3] = wrc;
                    localArea[4] = wlc;
                }
            }

            if(!GetFlagsAndMarkersAbove(yp, localArea, pid, 0, z))
            {
                x.FreeNodeAtomically();
                w.FreeNodeAtomically();
                if(!w.IsSentinel)
                {
                    wlc.FreeNodeAtomically();
                    wrc.FreeNodeAtomically();
                }
                if(isNotCheckZ)
                {
                    yp.FreeNodeAtomically();
                }
                return false;
            }
            return true;
        }

         private void BalanceTreeAfterDelete(
            RedBlackNode<TKey, TValue> x,
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            bool done = false, didMoveUp = false;

            // maintain Red-Black tree balance after deleting node
            while (x != _root && x.Color == RedBlackNodeType.Black && !done)
            {
                RedBlackNode<TKey, TValue> w;
                // determine sub tree from parent
                if (x == x.Parent.Left)
                {
                    w = x.Parent.Right;
                    if (w.Color == RedBlackNodeType.Red)
                    {
                        x.Parent.Color = RedBlackNodeType.Red;
                        w.Color = RedBlackNodeType.Black;
                        RotateLeft(x.Parent);
                        w = x.Parent.Right;
                        FixUpCase1(localArea, pid);
                    }
                    if (w.Left.Color == RedBlackNodeType.Black &&
                        w.Right.Color == RedBlackNodeType.Black)
                    {
                        // children are both black
                        // change parent to red
                        w.Color = RedBlackNodeType.Red;
                        // move up the tree
                        x = MoveDeleterUp(localArea, pid);
                    }
                    else
                    {
                        if (w.Right.Color == RedBlackNodeType.Black)
                        {
                            w.Left.Color = RedBlackNodeType.Black;
                            w.Color = RedBlackNodeType.Red;
                            RotateRight(w);
                            FixUpCase3(localArea, pid);
                            w = x.Parent.Right;
                        }
                        w.Color = x.Parent.Color;
                        x.Parent.Color = RedBlackNodeType.Black;
                        w.Right.Color = RedBlackNodeType.Black;
                        RotateLeft(x.Parent);
                        didMoveUp = ApplyMoveUpRule(localArea, pid);
                        done = true;
                    }
                }
                else
                {	// right subtree - same as code above with right and left swapped
                    w = x.Parent.Left;
                    if (w.Color == RedBlackNodeType.Red)
                    {
                        x.Parent.Color = RedBlackNodeType.Red;
                        w.Color = RedBlackNodeType.Black;
                        RotateRight(x.Parent);
                        w = x.Parent.Left;
                        FixUpCase1(localArea, pid);
                    }
                    if (w.Right.Color == RedBlackNodeType.Black &&
                        w.Left.Color == RedBlackNodeType.Black)
                    {
                        w.Color = RedBlackNodeType.Red;
                        x = MoveDeleterUp(localArea, pid);
                    }
                    else
                    {
                        if (w.Left.Color == RedBlackNodeType.Black)
                        {
                            w.Right.Color = RedBlackNodeType.Black;
                            w.Color = RedBlackNodeType.Red;
                            RotateLeft(w);
                            FixUpCase3(localArea, pid);
                            w = x.Parent.Left;
                        }
                        w.Color = x.Parent.Color;
                        x.Parent.Color = RedBlackNodeType.Black;
                        w.Left.Color = RedBlackNodeType.Black;
                        RotateRight(x.Parent);
                        didMoveUp = ApplyMoveUpRule(localArea, pid);
                        done = true;
                    }
                }
            }
            if(!didMoveUp)
            {
                x.Color = RedBlackNodeType.Black;

                //  release markers on local area
                var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
                var localAreaTopNode = (localArea[1].Parent == localArea[2]) ? localArea[2] : localArea[1];
                while(true)
                {
                    if(GetFlagsForMarkers(localAreaTopNode, pid, intentionMarkers, null))
                    {
                        break;
                    }
                }
                foreach(var node in intentionMarkers)
                {
                    node.Marker = Guid.Empty;
                }
                ReleaseFlagsAfterFailure(intentionMarkers, pid);

                //  correct relocated intention markers for other processes
                if (localArea[2].Marker != Guid.Empty 
                    && localArea[3].Marker == localArea[2].Marker
                    && localArea[1].Marker == Guid.Empty)
                {
                    localArea[1].Marker = localArea[2].Marker;
                    localArea[2].Marker = Guid.Empty;
                }
                if (localArea[2].Marker != Guid.Empty 
                    && localArea[4].Marker == localArea[2].Marker
                    && localArea[1].Marker == localArea[2].Marker)
                {
                    Console.WriteLine("FixUpCase4: This should not happen!");
                    while(true)
                    {
                        if(localArea[2].Parent.OccupyNodeAtomically())
                        {
                            break;
                        }
                    }
                    localArea[2].Parent.Marker =  localArea[2].Marker;
                    localArea[2].Parent.FreeNodeAtomically();
                }

                //  release flags on local area
                foreach (var node in localArea)
                {
                    node?.FreeNodeAtomically();
                }
            }
        }

        private bool GetFlagsAndMarkersAbove(
            RedBlackNode<TKey, TValue> start,
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid,
            int numAdditional,
            RedBlackNode<TKey, TValue> z = null) 
        {
            var markerPositions = new RedBlackNode<TKey, TValue>[4];

            if (!GetFlagsForMarkers(start, pid, markerPositions, z))
            {
                return false;
            }

            if (numAdditional == 0)
            {
                bool IsSetMarkerSuccess = setMarker(markerPositions.ToList(), pid, z);

                // release flags on four nodes after putting markers
                ReleaseFlagsAfterFailure(markerPositions, pid);

                return IsSetMarkerSuccess;
            }

            var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();

            // get additional marker(s) above
            RedBlackNode<TKey, TValue> firstnew = markerPositions[3].Parent;
            
            if (!IsIn(firstnew, pid) && !firstnew.OccupyNodeAtomically()) 
            {
                nodesToRelease.Add(markerPositions[0]);
                nodesToRelease.Add(markerPositions[1]);
                nodesToRelease.Add(markerPositions[2]);
                nodesToRelease.Add(markerPositions[3]);
                ReleaseFlagsAfterFailure(nodesToRelease, pid);
                return false;
            }
            if (firstnew != markerPositions[3].Parent && !IsSpacingRuleSatisfied(firstnew, pid, false, null)) 
            { 
                nodesToRelease.Add(markerPositions[0]);
                nodesToRelease.Add(markerPositions[1]);
                nodesToRelease.Add(markerPositions[2]);
                nodesToRelease.Add(markerPositions[3]);
                nodesToRelease.Add(firstnew);
                ReleaseFlagsAfterFailure(nodesToRelease, pid);
                return false;
            }

            if (numAdditional == 1)
            {
                firstnew.Marker = pid;
                nodesToRelease.Add(markerPositions[1]);
                nodesToRelease.Add(markerPositions[2]);
                nodesToRelease.Add(markerPositions[3]);
                nodesToRelease.Add(firstnew);

                localArea[1] = markerPositions[0];
                markerPositions[0].Marker = Guid.Empty;

                ReleaseFlagsAfterSuccess(nodesToRelease, pid);
                
                return true;;
            }

            if (numAdditional == 2)
            {
                // get additional marker above
                RedBlackNode<TKey, TValue> secondnew = firstnew.Parent;
                
                if (!IsIn(secondnew, pid) && !secondnew.OccupyNodeAtomically()) 
                {
                    nodesToRelease.Add(markerPositions[0]);
                    nodesToRelease.Add(markerPositions[1]);
                    nodesToRelease.Add(markerPositions[2]);
                    nodesToRelease.Add(markerPositions[3]);
                    nodesToRelease.Add(firstnew);
                    ReleaseFlagsAfterFailure(nodesToRelease, pid);
                    return false;
                }
                if (secondnew != firstnew.Parent && !IsSpacingRuleSatisfied(secondnew, pid, false, null)) 
                { 
                    nodesToRelease.Add(markerPositions[0]);
                    nodesToRelease.Add(markerPositions[1]);
                    nodesToRelease.Add(markerPositions[2]);
                    nodesToRelease.Add(markerPositions[3]);
                    nodesToRelease.Add(firstnew);
                    nodesToRelease.Add(secondnew);
                    ReleaseFlagsAfterFailure(nodesToRelease, pid);
                    return false;
                }
                firstnew.Marker = pid;
                secondnew.Marker = pid;

                nodesToRelease.Add(markerPositions[2]);
                nodesToRelease.Add(markerPositions[3]);
                nodesToRelease.Add(firstnew);
                nodesToRelease.Add(secondnew);

                markerPositions[0].Marker = Guid.Empty;
                markerPositions[1].Marker = Guid.Empty;

                ReleaseFlagsAfterSuccess(nodesToRelease, pid);
                
                return true;;
            }

            return true;
        }

        private bool setMarker(
            List<RedBlackNode<TKey, TValue>> nodesToMark, 
            Guid pid,
            RedBlackNode<TKey, TValue> z = null)
        {
            var nodesToUnMark = new List<RedBlackNode<TKey, TValue>>();

            RedBlackNode<TKey, TValue> node;
            for (var i = 0; i < 3; i++)
            {
                node = nodesToMark[i];
                if (!IsSpacingRuleSatisfied(node, pid, true, z)) 
                { 
                    foreach (var n in nodesToUnMark)
                    {
                        n.Marker = Guid.Empty;       
                    }
                    return false;
                }
                node.Marker = pid;
                nodesToUnMark.Add(node);
            }

            node = nodesToMark[3];
            if (!IsSpacingRuleSatisfied(node, pid, false, z)) 
            { 
                foreach (var n in nodesToUnMark)
                {
                    n.Marker = Guid.Empty;       
                }
                return false;
            }
            node.Marker = pid;

            return true;
        }

        private bool GetFlagsForMarkers(
            RedBlackNode<TKey, TValue> start,
            Guid pid,
            RedBlackNode<TKey, TValue>[] markerPositions,
            RedBlackNode<TKey, TValue> z = null) 
        {
            var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();

            markerPositions[0] = start.Parent;
            if (!GetFlagForMarker(markerPositions[0], start, nodesToRelease, pid, z))
            {
                return false;
            }

            markerPositions[1] = markerPositions[0].Parent;
            if (!GetFlagForMarker(markerPositions[1], markerPositions[0], nodesToRelease, pid, z))
            {
                return false;
            }

            markerPositions[2] = markerPositions[1].Parent;
            if (!GetFlagForMarker(markerPositions[2], markerPositions[1], nodesToRelease, pid, z))
            {
                return false;
            }

            markerPositions[3] = markerPositions[2].Parent;
            if (!GetFlagForMarker(markerPositions[3], markerPositions[2], nodesToRelease, pid, z))
            {
                return false;
            }
            return true;
        }

        private bool GetFlagForMarker(
            RedBlackNode<TKey, TValue> node,
            RedBlackNode<TKey, TValue> prevNode,
            List<RedBlackNode<TKey, TValue>> nodesToRelease,
            Guid pid,
            RedBlackNode<TKey, TValue> z = null) 
        {
            if (z == null || z != node)
            {
                if (!IsIn(node, pid) && !node.OccupyNodeAtomically())
                {
                    ReleaseFlagsAfterFailure(nodesToRelease, pid);
                    return false;
                }
                
                nodesToRelease.Add(node);

                // verify parent is unchanged
                if (node != prevNode.Parent)
                {
                    ReleaseFlagsAfterFailure(nodesToRelease, pid);
                    return false;
                }
            }
            return true;
        }


        private void FixUpCase1(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            //  correct relocated intention markers for other processes
            if (localArea[2].Marker != Guid.Empty 
                && localArea[3].Marker == localArea[2].Marker
                && localArea[1].Marker == Guid.Empty)
            {
                localArea[1].Marker = localArea[2].Marker;
                localArea[2].Marker = Guid.Empty;
            }
            if (localArea[2].Marker != Guid.Empty 
                && localArea[4].Marker == localArea[2].Marker
                && localArea[1].Marker == localArea[2].Marker)
            {
                Console.WriteLine("FixUpCase1: This should not happen!");
            }

            // Now correct local area and intention markers for the given process
            // release highest held intention marker (fifth intention marker)
            var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
            while(true)
            {
                if(GetFlagsForMarkers(localArea[2], pid, intentionMarkers, null))
                {
                    break;
                }
            }
            intentionMarkers[3].Marker  = Guid.Empty;
            ReleaseFlagsAfterFailure(intentionMarkers, pid);

            //  release flag on node not "moved"
            localArea[4].FreeNodeAtomically();
            // acquire marker on old sibling of x and free the flag
            localArea[2].Marker = pid;
            localArea[2].FreeNodeAtomically();

            RedBlackNode<TKey, TValue> neww = localArea[3];
            RedBlackNode<TKey, TValue> newwl = neww.Left;
            RedBlackNode<TKey, TValue> newwr = neww.Right;

            if (!IsIn(newwl, pid))
            {
                while(true)
                {
                    if(newwl.OccupyNodeAtomically())
                    {
                        break;
                    }
                }
            }
            if (!IsIn(newwr, pid))
            {
                while(true)
                {
                    if(newwr.OccupyNodeAtomically())
                    {
                        break;
                    }
                }
            } 

            localArea[2] = neww;
            if (neww == localArea[1].Right)
            {
                localArea[3] = newwl;
                localArea[4] = newwr;
            }
            else
            {
                localArea[3] = newwr;
                localArea[4] = newwl;
            }
        }

        private RedBlackNode<TKey, TValue> MoveDeleterUp(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            // Check for a moveUpStruct from another process (due to Move-Up rule). Get direct pointers

            // get  old local area
            RedBlackNode<TKey, TValue> oldx = localArea[0];
            RedBlackNode<TKey, TValue> oldp = localArea[1];
            RedBlackNode<TKey, TValue> oldw = localArea[2];
            RedBlackNode<TKey, TValue> oldwlc = localArea[3];
            RedBlackNode<TKey, TValue> oldwrc = localArea[4];

            var newx = oldp;
            localArea[0] = newx;

            // Extend intention markers (getting flags to set them) by one more
            // Also convert marker on oldgp to a flag i.e. set localArea[1] to oldgp
            while(true)
            {
                if(GetFlagsAndMarkersAbove(oldp, localArea, pid, 1))
                {
                    break;
                }
            }

            // get flags on rest of the new local area (w, wlc, wrc)
            var newp = newx.Parent;
            var neww = newx == newp.Left ? newp.Right : newp.Left;
            if (!IsIn(neww, pid))
            {
                while(true)
                {
                    if(neww.OccupyNodeAtomically())
                    {
                        break;
                    }
                }
            } 

            var newwlc = neww.Left;
            var newwrc = neww.Right;
            if (!IsIn(newwlc, pid))
            {
                while(true)
                {
                    if(newwlc.OccupyNodeAtomically())
                    {
                        break;
                    }
                }
            } 
            if (!IsIn(newwrc, pid))
            {
                while(true)
                {
                    if(newwrc.OccupyNodeAtomically())
                    {
                        break;
                    }
                }
            } 

            localArea[2] = neww;
            if (neww == localArea[1].Right)
            {
                localArea[3] = newwlc;
                localArea[4] = newwrc;
            }
            else
            {
                localArea[3] = newwrc;
                localArea[4] = newwlc;
            }

            // release flag on old local area
            List<RedBlackNode<TKey, TValue>> nodesToRelease = new List<RedBlackNode<TKey, TValue>>();
            nodesToRelease.Add(oldx);
            nodesToRelease.Add(oldw);
            nodesToRelease.Add(oldwlc);
            nodesToRelease.Add(oldwrc);
            ReleaseFlagsAfterSuccess(nodesToRelease, pid);

            return newx;
        }

        private void FixUpCase3(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            var newSiblingNewChild = (localArea[3] == localArea[1].Right) ? localArea[3].Left : localArea[3].Right;

            if (!IsIn(newSiblingNewChild, pid))
            {
                while(true)
                {
                    if(newSiblingNewChild.OccupyNodeAtomically())
                    {
                        break;
                    }
                }
            }

            //  correct relocated intention markers for other processes
            if (localArea[2].Marker != Guid.Empty 
                && localArea[1].Marker == localArea[2].Marker 
                && localArea[4].Marker == localArea[2].Marker )
            {
                localArea[3].Marker = localArea[2].Marker;
                localArea[1].Marker = Guid.Empty; 
            }

            if (localArea[2].Marker != Guid.Empty && localArea[3].Marker == localArea[2].Marker
                && newSiblingNewChild.Marker == localArea[2].Marker && localArea[1].Marker == Guid.Empty)
            {
                localArea[1].Marker = localArea[2].Marker;
                localArea[2].Marker = Guid.Empty;
            }

            if (localArea[2].Marker != Guid.Empty && localArea[3].Marker == localArea[2].Marker
                && newSiblingNewChild.Marker == localArea[2].Marker && localArea[1].Marker == localArea[2].Marker)
            {
                Console.WriteLine("FixUpCase3: This should not happen!");
            }

            // Correct Local area
            localArea[4].FreeNodeAtomically();

            var movedNode = localArea[3];
            localArea[2] = movedNode;

            if (movedNode == localArea[1].Right)
            {
                localArea[3] = movedNode.Left;
                localArea[4] = movedNode.Right;
            }
            else
            {
                localArea[3] = movedNode.Right;
                localArea[4] = movedNode.Left;
            }
        }

        private bool ApplyMoveUpRule(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            // Check in our local area to see if two processes beneath
            // use have been brought too close together by our rotations. 
            // The three cases correspond to Figures 17, 18 and 19.
            var w = localArea[4];
            var wChild_p1 = w.Right;
            var wChild_p2 = w.Left;
            if(w == w.Parent.Left)
            {
                wChild_p1 = w.Left;
                wChild_p2 = w.Right;
            }

            var case1 = w.Marker == w.Parent.Marker &&
                w.Marker == wChild_p1.Marker &&
                w.Marker != Guid.Empty &&
                wChild_p2.Marker != Guid.Empty;

            var case2 =  wChild_p1.Marker == w.Marker &&
                w.Marker != Guid.Empty &&
                wChild_p2.Marker != Guid.Empty;

            var case3 = w.Marker == Guid.Empty &&
                wChild_p1.Marker != Guid.Empty &&
                wChild_p2.Marker != Guid.Empty;

            if(case1 || case2 || case3)
            {
                Console.WriteLine("case1 || case2 || case3");
                // Let pid release their markers ??????????? or pass their markers as well?????
                var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
                while(true)
                {
                    if(GetFlagsForMarkers(w.Parent, pid, intentionMarkers, null))
                    {
                        break;
                    }
                }
                foreach(var node in intentionMarkers)
                {
                    node.Marker = Guid.Empty;
                }
                ReleaseFlagsAfterFailure(intentionMarkers, pid);

                // Build structure listing the nodes we hold flags on
                // (moveUpStruct) and specifying the PID of the other
                // "too-close" process (marker[left[w]) and the goal (GP). 
                // Make structure available to process id: marker[right[w]]

                var newMoveUpStruct = new MoveUpStruct<TKey, TValue>();
                newMoveUpStruct.Nodes.Add(localArea[0]);
                newMoveUpStruct.Nodes.Add(localArea[1]);
                newMoveUpStruct.Nodes.Add(localArea[2]);    // top node
                newMoveUpStruct.Nodes.Add(localArea[3]);
                newMoveUpStruct.Nodes.Add(localArea[4]);
                newMoveUpStruct.PidToIgnore = wChild_p2.Marker;
                // newMoveUpStruct.Gp = w.Parent.Parent ???????????? Gp is not locked, what if it changes

                var inheritedMoveUpStructs = new List<MoveUpStruct<TKey, TValue>>();

                if (moveUpStructDict.ContainsKey(pid))
                {
                    inheritedMoveUpStructs = moveUpStructDict[pid];
                }
                inheritedMoveUpStructs.Add(newMoveUpStruct);

                moveUpStructDict.TryAdd(wChild_p1.Marker, inheritedMoveUpStructs);
                
            }
            return false;
        }
    }
}