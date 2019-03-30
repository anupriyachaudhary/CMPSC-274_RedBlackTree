using System;
using System.Collections.Generic;
using System.Linq;

namespace ConcurrentRedBlackTree
{
    public partial class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
        private readonly Dictionary<Guid, object> locks = new Dictionary<Guid, object>();

        private Dictionary<Guid, List<MoveUpStruct<TKey, TValue>>> moveUpStructDict
            = new Dictionary<Guid, List<MoveUpStruct<TKey, TValue>>>();

        public bool Remove(TKey key)
        {
            Guid pid = Guid.NewGuid();
            //moveUpStructDict.Add(pid, new List<MoveUpStruct<TKey, TValue>>());
            locks.Add(pid, new object());
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
                moveUpStructDict.Remove(pid);
                locks.Remove(pid);
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
                    return false;
                }
                // check if correct node is locked
                if(z.Marker != Guid.Empty)
                {
                    z.FreeNodeAtomically();
                    return false;
                }

                // Find key-order successor, locked y is returned
                y = FindSuccessor(z);               
                if(y == null)
                {
                    return false;
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

            x = y.Left.IsSentinel ? y.Right  : y.Left;

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
                while(!GetFlagsForMarkers(localArea[1], pid, intentionMarkers, z));
                foreach (var node in intentionMarkers)
                {
                    node.Marker  = Guid.Empty;
                }
                ReleaseFlags(pid, false, intentionMarkers.ToList());
                
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

                //  correct relocated intention markers for other processes
                bool isGPmarkerChanged = false;
                // ????????????
                if (localArea[1].Marker != Guid.Empty 
                    && localArea[1].Marker == localArea[2].Marker 
                    && localArea[2].Marker == localArea[4].Marker )
                {
                    localArea[2].Parent.Marker =  localArea[2].Marker;
                    isGPmarkerChanged = true;
                }
                if (localArea[2].Marker != Guid.Empty && localArea[2].Marker == localArea[3].Marker )
                {
                    localArea[1].Marker = localArea[2].Marker;
                    localArea[2].Marker = Guid.Empty;
                }

                //  release markers on local area
                var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
                var localAreaTopNode = localArea[1];
                if(localAreaTopNode.Parent == localArea[2])
                {
                    localAreaTopNode = localArea[2];
                }
                while(!GetFlagsForMarkers(localAreaTopNode, pid, intentionMarkers, null));
                if(isGPmarkerChanged)
                { 
                    intentionMarkers[0].Marker = localArea[2].Marker;
                }
                else
                {
                    intentionMarkers[0].Marker = Guid.Empty;
                }
                intentionMarkers[1].Marker = Guid.Empty;
                intentionMarkers[2].Marker = Guid.Empty;
                intentionMarkers[3].Marker = Guid.Empty;
                ReleaseFlags(pid, false, intentionMarkers.ToList());

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
                ReleaseFlags(pid, false, markerPositions.ToList());

                return IsSetMarkerSuccess;
            }

            else if (numAdditional == 1)
            {
                var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();

                // get additional marker(s) above
                RedBlackNode<TKey, TValue> firstnew = markerPositions[3].Parent;
                
                if (!IsIn(firstnew, pid) && !firstnew.OccupyNodeAtomically()) 
                {
                    nodesToRelease.Add(markerPositions[0]);
                    nodesToRelease.Add(markerPositions[1]);
                    nodesToRelease.Add(markerPositions[2]);
                    nodesToRelease.Add(markerPositions[3]);
                    ReleaseFlags(pid, false, nodesToRelease.ToList());
                    return false;
                }
                if (firstnew != markerPositions[3].Parent && !SpacingRuleIsSatisfied(firstnew, pid, false, null)) 
                { 
                    nodesToRelease.Add(markerPositions[0]);
                    nodesToRelease.Add(markerPositions[1]);
                    nodesToRelease.Add(markerPositions[2]);
                    nodesToRelease.Add(markerPositions[3]);
                    nodesToRelease.Add(firstnew);
                    ReleaseFlags(pid, false, nodesToRelease.ToList());
                    return false;
                }
                firstnew.Marker = pid;

                nodesToRelease.Add(markerPositions[1]);
                nodesToRelease.Add(markerPositions[2]);
                nodesToRelease.Add(markerPositions[3]);
                nodesToRelease.Add(firstnew);

                localArea[1] = markerPositions[0];
                markerPositions[0].Marker = Guid.Empty;

                ReleaseFlags(pid, true, nodesToRelease);
                
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
                if (!SpacingRuleIsSatisfied(node, pid, true, z)) 
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
            if (!SpacingRuleIsSatisfied(node, pid, false, z)) 
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
                    ReleaseFlags(pid, false, nodesToRelease);
                    return false;
                }
                
                nodesToRelease.Add(node);

                // verify parent is unchanged
                if (node != prevNode.Parent)
                {
                    ReleaseFlags(pid, false, nodesToRelease);
                    return false;
                }
            }
            return true;
        }

        private bool IsIn(RedBlackNode<TKey, TValue> node, Guid pid)
        {
            List<MoveUpStruct<TKey, TValue>> moveUpStructList;
            if (moveUpStructDict.ContainsKey(pid))
            {
                moveUpStructList = moveUpStructDict[pid];
            }
            else
            {
                return false;
            }
            var moveUpStruct = moveUpStructList[moveUpStructList.Count - 1];

            foreach (var n in moveUpStruct.Nodes)
            {
                if (node == n)
                {
                    return true;
                }
            }
            
            return false;
        }

        private void ReleaseFlags(
            Guid pid, 
            bool success, 
            List<RedBlackNode<TKey, TValue>> nodesToRelease)
        {
            // new List empty ?????
            foreach (var nd in nodesToRelease)
            {
                // release flag after successfully moving up
                if(success)
                {
                    if(!IsIn(nd, pid))
                    {
                        nd.FreeNodeAtomically();
                    }
                    //nd is in inherited local area
                    else
                    {
                        if(IsGoalNode(nodesToRelease, pid))
                        {
                            // release unneeded flags in moveUpStruct and discard moveUpStruct
                            foreach (var moveUpStruct in  moveUpStructDict[pid])
                            {
                                foreach (var node in moveUpStruct.Nodes)
                                {
                                    node?.FreeNodeAtomically();
                                }
                            }
                            moveUpStructDict.Remove(pid);
                        }                     
                    }
                }
                // release flag after failing to move up
                else
                {
                    if(!IsIn(nd, pid))
                    {
                        nd.FreeNodeAtomically();
                    }
                }
            }
        }

        private bool IsGoalNode(List<RedBlackNode<TKey, TValue>> nodesToRelease, Guid pid)
        {
            List<MoveUpStruct<TKey, TValue>> moveUpStructList;
            if (moveUpStructDict.ContainsKey(pid))
            {
                moveUpStructList = moveUpStructDict[pid];
            }
            else
            {
                return false;
            }
            var moveUpStruct = moveUpStructList[moveUpStructList.Count - 1];
            
            // Can Gp be changed by rotation ?????????
            var gp = moveUpStruct.Nodes[2].Parent;

            // Hold a flag on root
            while(true)
            {
                gp.OccupyNodeAtomically();
                if(gp == moveUpStruct.Nodes[2].Parent)
                {
                    break;
                }
                gp.FreeNodeAtomically();
            }
            
            foreach (var node in nodesToRelease)
            {
                if (gp == node)
                {
                    return true;
                }
            }
            return false;
        }

        private bool SpacingRuleIsSatisfied(
            RedBlackNode<TKey, TValue> t,
            Guid pid,
            bool isParentOccupied,
            RedBlackNode<TKey, TValue> z = null)
        {
            // we already hold flags on both t and z.
            bool isMarkerAllowed  = true;

            // check that t has no marker set
            if(z == null || z != t)
            {
                if (t.Marker != Guid.Empty)
                {
                    return false;
                }
            }

            // check that t's parent has no flag or marker
            RedBlackNode<TKey, TValue> tp = t.Parent;
            
            if(z == null || z != tp)
            {
                if (!isParentOccupied)
                {
                    if (!IsIn(tp, pid) && !tp.OccupyNodeAtomically())
                    {
                        return false;
                    }
                    // verify parent is unchanged
                    if (tp != t.Parent)
                    {
                        tp.FreeNodeAtomically();
                        return false;
                    }
                }

                if (tp.Marker != Guid.Empty)
                {
                    tp.FreeNodeAtomically();
                    return false;
                }
            }
            
            //check that t's sibling has no flag or marker or PIDtoIgnore
            var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();
            var ts = (t == tp.Left ? tp.Right : tp.Left);

            if (!IsIn(ts, pid) && !ts.OccupyNodeAtomically())
            {
                if(z == null || z != tp)
                {
                    nodesToRelease.Add(tp);
                    ReleaseFlags(pid, false, nodesToRelease);
                }
                return false;
            }

            Guid PIDtoIgnore;
            bool IsTooCloseProcess = getPIDtoIgnore(pid, PIDtoIgnore);
            if (ts.Marker != Guid.Empty && (!IsTooCloseProcess || ts.Marker != PIDtoIgnore))
            {
                isMarkerAllowed = false;
            }

            // release flags on ts and tp
            nodesToRelease.Add(ts);
            if(z == null || z != tp)
            {
                nodesToRelease.Add(tp);
            }
            ReleaseFlags(pid, false, nodesToRelease);

            return isMarkerAllowed;
        }

        private bool getPIDtoIgnore(Guid pid, Guid PIDtoIgnore)
        {
            if (moveUpStructDict.ContainsKey(pid))
            {
                var moveUpStructList = moveUpStructDict[pid];
                var moveUpStruct = moveUpStructList[moveUpStructList.Count - 1];
                PIDtoIgnore = moveUpStruct.PidToIgnore;  
                return true;
            }
            return  false; 
        }

        private void FixUpCase1(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            //  correct relocated intention markers for other processes
            if (localArea[2].Marker != Guid.Empty && localArea[2].Marker == localArea[3].Marker)
            {
                localArea[1].Marker = localArea[2].Marker;
                localArea[2].Marker = Guid.Empty;
            }

            // Now correct local area and intention markers for the given process
            // release highest held intention marker (fifth intention marker)
            var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
            while(!GetFlagsForMarkers(localArea[2], pid, intentionMarkers, null));
            intentionMarkers[3].Marker  = Guid.Empty;
            ReleaseFlags(pid, false, intentionMarkers.ToList());

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
                while(!newwl.OccupyNodeAtomically());
            }
            if (!IsIn(newwr, pid))
            {
                while(!newwr.OccupyNodeAtomically());
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
            while(!GetFlagsAndMarkersAbove(oldp, localArea, pid, 1));

            // get flags on rest of the new local area (w, wlc, wrc)
            var newp = newx.Parent;
            var neww = newx == newp.Left ? newp.Right : newp.Left;

            if (!IsIn(neww, pid))
            {
                while(!neww.OccupyNodeAtomically());
            } 
            var newwlc = neww.Left;
            var newwrc = neww.Right;
            if (!IsIn(newwlc, pid))
            {
                while(!newwlc.OccupyNodeAtomically());
            } 
            if (!IsIn(newwrc, pid))
            {
                while(!newwrc.OccupyNodeAtomically());
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
            ReleaseFlags(pid, true, nodesToRelease);

            return newx;
        }

        private void FixUpCase3(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid pid)
        {
            //  correct relocated intention markers for other processes
            if (localArea[1].Marker != Guid.Empty 
                && localArea[1].Marker == localArea[2].Marker 
                && localArea[2].Marker == localArea[4].Marker )
            {
                localArea[3].Marker = localArea[2].Marker;
                localArea[1].Marker = Guid.Empty; 
            }

            var newSiblingNewChild = (localArea[3] == localArea[1].Right) ? localArea[3].Left : localArea[3].Right;

            if (!IsIn(newSiblingNewChild, pid))
            {
                while(!newSiblingNewChild.OccupyNodeAtomically());
            }

            if (localArea[2].Marker != Guid.Empty 
                && localArea[2].Marker == localArea[3].Marker
                && localArea[2].Marker == newSiblingNewChild.Marker)
            {
                localArea[1].Marker = localArea[2].Marker;
                localArea[2].Marker = Guid.Empty;
            }

            //  release flag on old wr
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
            var x = localArea[0];
            var w = localArea[2];
            var wc1 = localArea[3];
            var wc2 = localArea[4];

            var case1 = w.Marker == w.Parent.Marker &&
                w.Marker == wc2.Marker &&
                w.Marker != Guid.Empty &&
                wc1.Marker != Guid.Empty;

            var case2 = w.Marker == wc2.Marker &&
                w.Marker != Guid.Empty &&
                wc1.Marker != Guid.Empty;

            var case3 = w.Marker == Guid.Empty &&
                wc1.Marker != Guid.Empty &&
                wc2.Marker != Guid.Empty;
            if(case1 || case2 || case3)
            {
                // Let pid release their markers ??????????? or pass their markers as well?????
                var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
                while(!GetFlagsForMarkers(localArea[2], pid, intentionMarkers, null))
                ReleaseFlags(pid, false, intentionMarkers.ToList());

                // Build structure listing the nodes we hold flags on
                // (moveUpStruct) and specifying the PID of the other
                // "too-close" process (marker[left[w]) and the goal (GP). 
                // Make structure available to process id: marker[right[w]]

                var newMoveUpStruct = new MoveUpStruct<TKey, TValue>();
                newMoveUpStruct.Nodes.Add(localArea[0]);
                newMoveUpStruct.Nodes.Add(localArea[1]);
                newMoveUpStruct.Nodes.Add(localArea[2]);
                newMoveUpStruct.Nodes.Add(localArea[3]);
                newMoveUpStruct.Nodes.Add(localArea[4]);
                newMoveUpStruct.PidToIgnore = w.Left.Marker;
                // newMoveUpStruct.Gp = localArea[2].Parent ???????????? Gp is not locked, what if it changes

                var inheritedMoveUpStructs = new List<MoveUpStruct<TKey, TValue>>();

                if (moveUpStructDict.ContainsKey(pid))
                {
                    inheritedMoveUpStructs = GetMoveUpStructWithLock(pid);
                }
                inheritedMoveUpStructs.Add(newMoveUpStruct);

                moveUpStructDict[w.Right.Marker] = inheritedMoveUpStructs;
            }
            return false;
        }

        private void UpdateMoveUpStructWithLock(Guid pid, MoveUpStruct<TKey, TValue> moveUpStruct)
        {
            lock (locks[pid])
            {
                moveUpStructDict[pid].Add( moveUpStruct);
            }
        }

        private List<MoveUpStruct<TKey, TValue>> GetMoveUpStructWithLock(Guid pid)
        {
            lock (locks[pid])
            {
                return moveUpStructDict[pid];
            }
        }
    }
}