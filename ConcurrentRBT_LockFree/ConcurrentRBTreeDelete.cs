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
            moveUpStructDict.Add(pid, new List<MoveUpStruct<TKey, TValue>>());
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
        
        private bool Delete(TKey key, Guid pid)
        {
            RedBlackNode<TKey, TValue> y = null, z = null, x = null;
            var localArea = new RedBlackNode<TKey, TValue>[5];
            var intentionMarkers = new RedBlackNode<TKey, TValue>[4];
            
            while (true)
            {
                z = GetNode(key);

                if(z == null || z.Marker != Guid.Empty)
                {
                    return false;
                }

                // Hold a flag on z
                if (!z.OccupyNodeAtomically())
                {
                    continue;
                }

                // check if correct node is locked
                if((z.Key.CompareTo(key) != 0) || GetNode(key) == null || z.Marker != Guid.Empty)
                {
                    z.FreeNodeAtomically();
                    return false;
                }

                // Find key-order successor
                if (z.Left.IsSentinel || z.Right.IsSentinel)
                {
                    y = z;
                }
                else
                {    
                    y = FindSuccessor(z);               
                }

                // if z and y are different than hold a flag on y as well
                if(z != y)
                {
                    if(!y.OccupyNodeAtomically())
                    {
                        z.FreeNodeAtomically();
                        continue;
                    }

                    var node = FindSuccessor(z);
                    if(node != y)
                    {
                        z.FreeNodeAtomically();
                        y.FreeNodeAtomically();
                        continue;
                    }
                }
                //we  now hold a flag on y and z

                if(!SetupLocalAreaForDelete(y, localArea, intentionMarkers, pid, z))
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
                z.FreeNodeAtomically();
                y.FreeNodeAtomically();     //?????????????
            }

            if (y.Color == RedBlackNodeType.Black)
            {
                BalanceTreeAfterDelete(x, localArea, intentionMarkers, pid);
            }
            else
            {
                 // Release flags and markers of local area
                foreach (var node in localArea)
                {
                    node?.FreeNodeAtomically();
                }
                foreach (var node in intentionMarkers)
                {
                    node.Marker = Guid.Empty;
                }
            }

            return true;
        }

        private RedBlackNode<TKey, TValue> FindSuccessor(RedBlackNode<TKey, TValue> z)
        {
            var node = z.Right;
            while (!node.Left.IsSentinel)
            {
                node = node.Left;
            }
            return node;
        }

        private bool SetupLocalAreaForDelete(
            RedBlackNode<TKey, TValue> y,
            RedBlackNode<TKey, TValue>[] localArea,
            RedBlackNode<TKey, TValue>[] intentionMarkers,
            Guid pid,
            RedBlackNode<TKey, TValue> z = null)
        {
            var x = y.Left.IsSentinel ? y.Right : y.Left;
            bool isNotCheckZ = (z == null || y != z);

            // occupy the node which will replace y
            if (!x.OccupyNodeAtomically())
            {
                return false;
            }
            localArea[0] = x;

            var yp = y.Parent;

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
            }

            localArea[3] = wlc;
            localArea[4] = wrc;

            if(!GetFlagsAndMarkersAbove(yp, localArea, intentionMarkers, pid, 0))
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
            return true;
        }

         private void BalanceTreeAfterDelete(
            RedBlackNode<TKey, TValue> x,
            RedBlackNode<TKey, TValue>[] localArea,
            RedBlackNode<TKey, TValue>[] intentionMarkers,
            Guid myPID)
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
                        FixUpCase1(localArea, intentionMarkers, myPID);
                    }
                    if (w.Left.Color == RedBlackNodeType.Black &&
                        w.Right.Color == RedBlackNodeType.Black)
                    {
                        // children are both black
                        // change parent to red
                        w.Color = RedBlackNodeType.Red;
                        // move up the tree
                        x = MoveDeleterUp(localArea, intentionMarkers, myPID);
                    }
                    else
                    {
                        if (w.Right.Color == RedBlackNodeType.Black)
                        {
                            w.Left.Color = RedBlackNodeType.Black;
                            w.Color = RedBlackNodeType.Red;
                            RotateRight(w);
                            FixUpCase3(localArea, myPID);
                            w = x.Parent.Right;
                        }
                        x.Parent.Color = RedBlackNodeType.Black;
                        w.Color = x.Parent.Color;
                        w.Right.Color = RedBlackNodeType.Black;
                        RotateLeft(x.Parent);
                        didMoveUp = ApplyMoveUpRule(localArea, myPID);
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
                        FixUpCase1(localArea, intentionMarkers, myPID);
                    }
                    if (w.Right.Color == RedBlackNodeType.Black &&
                        w.Left.Color == RedBlackNodeType.Black)
                    {
                        w.Color = RedBlackNodeType.Red;
                        x = MoveDeleterUp(localArea, intentionMarkers, myPID);
                    }
                    else
                    {
                        if (w.Left.Color == RedBlackNodeType.Black)
                        {
                            w.Right.Color = RedBlackNodeType.Black;
                            w.Color = RedBlackNodeType.Red;
                            RotateLeft(w);
                            FixUpCase3(localArea, myPID);
                            w = x.Parent.Left;
                        }
                        w.Color = x.Parent.Color;
                        x.Parent.Color = RedBlackNodeType.Black;
                        w.Left.Color = RedBlackNodeType.Black;
                        didMoveUp = ApplyMoveUpRule(localArea, myPID);
                        done = true;
                    }
                }
            }
            if(!didMoveUp)
            {
                x.Color = RedBlackNodeType.Black;


                // fix relocated markers ????????????
            }
        }

        private bool GetFlagsAndMarkersAbove(
            RedBlackNode<TKey, TValue> start,
            RedBlackNode<TKey, TValue>[] localArea,
            RedBlackNode<TKey, TValue>[] intentionMarkers,
            Guid pid,
            int numAdditional,
            RedBlackNode<TKey, TValue> z = null) 
        {
            List<RedBlackNode<TKey, TValue>> nodesToRelease = new List<RedBlackNode<TKey, TValue>>();

            var markerPositions = new RedBlackNode<TKey, TValue>[4];

            if (!GetFlagsForMarkers(start, pid, markerPositions, z))
            {
                return false;
            }

            if (numAdditional == 0)
            {
                // what if z is encountered, lock for spacing rule problem??????
                bool IsSetMarkerSuccess = setMarker(markerPositions.ToList(), pid);

                if (IsSetMarkerSuccess)
                {
                    intentionMarkers[0] = pos1;
                    intentionMarkers[1] = pos2;
                    intentionMarkers[2] = pos3;
                    intentionMarkers[3] = pos4;
                }

                nodesToRelease.Add(pos1);
                nodesToRelease.Add(pos2);
                nodesToRelease.Add(pos3);
                nodesToRelease.Add(pos4);
                ReleaseFlags(pid, false, nodesToRelease);

                return IsSetMarkerSuccess;
            }
            else if (numAdditional == 1)
            {
                bool IsAdditionalMarkerSuccessful = true;

                // get additional marker(s) above
                RedBlackNode<TKey, TValue> firstnew = pos4.Parent;
                
                if (!IsIn(firstnew, pid) && !firstnew.OccupyNodeAtomically()) 
                {
                    IsAdditionalMarkerSuccessful = false;
                }
                if (firstnew != pos4.Parent && !SpacingRuleIsSatisfied(firstnew, pid)) 
                { 
                    IsAdditionalMarkerSuccessful = false;
                }
                if (!IsIn(firstnew, pid))
                {
                    firstnew.Marker = pid;
                }

                if (IsAdditionalMarkerSuccessful)
                {
                    intentionMarkers[0] = pos2;
                    intentionMarkers[1] = pos3;
                    intentionMarkers[2] = pos4;
                    intentionMarkers[3] = firstnew;
                    localArea[1] = pos1;
                }

                nodesToRelease.Add(pos2);
                nodesToRelease.Add(pos3);
                nodesToRelease.Add(pos4);
                nodesToRelease.Add(firstnew);
                ReleaseFlags(pid, true, nodesToRelease);

                return IsAdditionalMarkerSuccessful;
            }

            return true;
        }

        private bool setMarker(
            List<RedBlackNode<TKey, TValue>> nodesToMark, 
            Guid pid)
        {
            var nodesToUnMark = new List<RedBlackNode<TKey, TValue>>();
            bool IsMarkSuccess = true;

            foreach (var node in nodesToMark)
            {
                if (!SpacingRuleIsSatisfied(node, pid)) 
                { 
                    foreach (var n in nodesToUnMark)
                    {
                        if (!IsIn(n, pid))
                        {
                            n.Marker = Guid.Empty;
                        }         
                    }
                    return false;
                }
                else if (!IsIn(node, pid))
                {
                    node.Marker = pid;
                    IsMarkSuccess = true;
                    nodesToUnMark.Add(node);
                }
            }

            return IsMarkSuccess;
        }

        private bool GetFlagsForMarkers(
            RedBlackNode<TKey, TValue> start,
            Guid pid,
            RedBlackNode<TKey, TValue>[] markerPositions,
            RedBlackNode<TKey, TValue> z = null) 
        {
            var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();

            markerPositions[0] = start.Parent;
            if ((z == null || z != markerPositions[0])
                && !IsIn(markerPositions[0], pid)
                && !markerPositions[0].OccupyNodeAtomically())
            {
                return false;
            }
            
            if(z == null || z != markerPositions[0])
            {
                nodesToRelease.Add(markerPositions[0]);
            }

            // verify parent is unchanged
            if ((z == null || z != markerPositions[0])
                && markerPositions[0] != start.Parent)
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }

            markerPositions[1] = markerPositions[0].Parent;
            if ((z == null || z != markerPositions[1])
                && !IsIn(markerPositions[1], pid)
                && !markerPositions[1].OccupyNodeAtomically())
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }

            if(z == null || z != markerPositions[1])
            {
                nodesToRelease.Add(markerPositions[1]);
            }

            // verify parent is unchanged
            if ((z == null || z != markerPositions[1])
                && markerPositions[1] != markerPositions[0].Parent)
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }

            markerPositions[2] = markerPositions[1].Parent;
            if ((z == null || z != markerPositions[2])
                && !IsIn(markerPositions[2], pid)
                && !markerPositions[2].OccupyNodeAtomically())
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }
            
            if(z == null || z != markerPositions[2])
            {
                nodesToRelease.Add(markerPositions[2]);
            }

            // verify parent is unchanged
            if ((z == null || z != markerPositions[2])
                && markerPositions[2] != markerPositions[1].Parent)
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }

            markerPositions[3] = markerPositions[2].Parent;
            if ((z == null || z != markerPositions[3])
                && !IsIn(markerPositions[3], pid)
                && !markerPositions[3].OccupyNodeAtomically())
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }

            if(z == null || z != markerPositions[3])
            {
                nodesToRelease.Add(markerPositions[3]);
            }

            // verify parent is unchanged
            if ((z == null || z != markerPositions[3])
                && markerPositions[3] != markerPositions[2].Parent)
            {
                ReleaseFlags(pid, false, nodesToRelease);
                return false;
            }

            return true;
        }

        private bool IsIn(RedBlackNode<TKey, TValue> node, Guid pid)
        {
            MoveUpStruct<TKey, TValue> moveUpStruct;
            if (moveUpStructDict.ContainsKey(pid))
            {
                moveUpStruct = GetMoveUpStructWithLock(pid);
                foreach (var n in moveUpStruct.Nodes)
                {
                    if (node == n)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private void ReleaseFlags(
            Guid myPID, 
            bool success, 
            List<RedBlackNode<TKey, TValue>> nodesToRelease)
        {
            foreach (var nd in nodesToRelease)
            {
                // release flag after successfully moving up
                if(success)
                {
                    if(!IsIn(nd, myPID))
                    {
                        nd.FreeNodeAtomically();
                    }
                    //nd is in inherited local area
                    else
                    {
                        if(IsGoalNode(nd, myPID))
                        {
                            // release unneeded flags in moveUpStruct and discard moveUpStruct

                        }                     
                    }
                }
                // release flag after failing to move up
                else
                {
                    if(!IsIn(nd, myPID))
                    {
                        nd.FreeNodeAtomically();
                    }
                }
            }
        }

        private bool IsGoalNode(RedBlackNode<TKey, TValue> nd, Guid myPID)
        {
            MoveUpStruct<TKey, TValue> moveUpStruct;
            if (moveUpStructDict.ContainsKey(myPID))
            {
                moveUpStruct = GetMoveUpStructWithLock(myPID);
                if (nd == moveUpStruct.Gp)
                {
                    return true;
                }
            }
            return false;
        }

        private bool SpacingRuleIsSatisfied(
            RedBlackNode<TKey, TValue> t,
            Guid pid,
            RedBlackNode<TKey, TValue> z = null)
        {
            // we already hold flags on both t and z.
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
                if (tp.Marker != Guid.Empty)
                {
                    tp.FreeNodeAtomically();
                    return false;
                }
            }
            

            //check that t's sibling has no flag or marker
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

            bool sibHasMarkerToBeIgnored = false;
            if (moveUpStructDict.ContainsKey(pid))
            {
                MoveUpStruct<TKey, TValue> moveUpStruct = GetMoveUpStructWithLock(pid);
                foreach (var id in moveUpStruct.PidToIgnore)
                {
                    if (ts.Marker == id)
                    {
                        sibHasMarkerToBeIgnored = true;
                    }
                }
            }

            if (ts.Marker != Guid.Empty && !sibHasMarkerToBeIgnored) // ??????????????? Don't understand why only sib checked for PIDtoIgnore
            {
                nodesToRelease.Clear();
                nodesToRelease.Add(ts);
                nodesToRelease.Add(tp);
                ReleaseFlags(pid, false, nodesToRelease);

                return false;
            }

            nodesToRelease.Clear();
            nodesToRelease.Add(tp);
            nodesToRelease.Add(ts);
            ReleaseFlags(pid, false, nodesToRelease);

            return true;
        }

        private void FixUpCase1(
            RedBlackNode<TKey, TValue>[] localArea,
            RedBlackNode<TKey, TValue>[] intentionMarkers,
            Guid myPID)
        {
            //  release flag on old wr
            localArea[4].FreeNodeAtomically();
            // acquire marker on old sibling of x and free the flag
            localArea[2].Marker = myPID;
            localArea[2].FreeNodeAtomically();

            RedBlackNode<TKey, TValue> neww = localArea[3];
            RedBlackNode<TKey, TValue> newwl = neww.Left;
            RedBlackNode<TKey, TValue> newwr = neww.Right;

            if (!IsIn(newwl, myPID))
            {
                while(!newwl.OccupyNodeAtomically());
            } 
            if (!IsIn(newwr, myPID))
            {
                while(!newwr.OccupyNodeAtomically());
            }

            localArea[2] = neww;
            localArea[3] = newwl;
            localArea[4] = newwr;

            // release highest held intention marker (fifth intention marker)
            intentionMarkers[3].Marker  = Guid.Empty;

            intentionMarkers[3] = intentionMarkers[2];
            intentionMarkers[2] = intentionMarkers[1];
            intentionMarkers[1] = intentionMarkers[0];
            intentionMarkers[0] = localArea[2];

            //  correct relocated intention markers for other processes ????????????????
        }

        private RedBlackNode<TKey, TValue> MoveDeleterUp(
            RedBlackNode<TKey, TValue>[] localArea,
            RedBlackNode<TKey, TValue>[] intentionMarkers,
            Guid myPID)
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
            while(!GetFlagsAndMarkersAbove(oldp, localArea, intentionMarkers, myPID, 1));

            // get flags on rest of the new local area (w, wlc, wrc)
            var newp = newx.Parent;
            var neww = newx == newp.Left ? newp.Right : newp.Left;

            if (!IsIn(neww, myPID))
            {
                while(!neww.OccupyNodeAtomically());
            } 
            var newwlc = neww.Left;
            var newwrc = neww.Right;
            if (!IsIn(newwlc, myPID))
            {
                while(!newwlc.OccupyNodeAtomically());
            } 
            if (!IsIn(newwrc, myPID))
            {
                while(!newwrc.OccupyNodeAtomically());
            } 
            localArea[2] = neww;
            localArea[3] = newwlc;
            localArea[4] = newwrc;

            // release flag on old local area
            List<RedBlackNode<TKey, TValue>> nodesToRelease = new List<RedBlackNode<TKey, TValue>>();
            nodesToRelease.Add(oldx);
            nodesToRelease.Add(oldwlc);
            nodesToRelease.Add(oldwlc);
            nodesToRelease.Add(oldwrc);
            ReleaseFlags(myPID, true, nodesToRelease);

            return newx;
        }

        private void FixUpCase3(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid myPID)
        {
            //  release flag on old wr
            localArea[4].FreeNodeAtomically();

            RedBlackNode<TKey, TValue> neww = localArea[3];
            RedBlackNode<TKey, TValue> newwr = localArea[2];

            RedBlackNode<TKey, TValue> newwl = neww.Left;
            if (!IsIn(newwl, myPID))
            {
                while(!newwl.OccupyNodeAtomically());
            } 

            localArea[2] = neww;
            localArea[3] = newwl;
            localArea[4] = newwr;

            //  correct relocated intention markers for other processes ????????????????
        }

        private bool ApplyMoveUpRule(
            RedBlackNode<TKey, TValue>[] localArea,
            Guid myPID)
        {
            // Check in our local area to see if two processes beneath
            // use have been brought too close together by our rotations. 
            // The three cases correspond to Figures 17, 18 and 19.
            var x = localArea[0];
            var w = localArea[4];

            var case1 = w.Marker == w.Parent.Marker &&
                w.Marker == w.Right.Marker &&
                w.Marker != Guid.Empty &&
                w.Left.Marker != Guid.Empty;

            var case2 = w.Marker == w.Right.Marker &&
                w.Marker != Guid.Empty &&
                w.Left.Marker != Guid.Empty;

            var case3 = w.Marker == Guid.Empty &&
                w.Left.Marker != Guid.Empty &&
                w.Right.Marker != Guid.Empty;
            if(case1 || case2 || case3)
            {
                // Build structure listing the nodes we hold flags on
                // (moveUpStruct) and specifying the PID of the other
                // "too-close" process (marker[left[w]) and the goal (GP). 
                // Make structure available to process id: marker[right[w]]

                MoveUpStruct<TKey, TValue> moveUpStruct;
                if (moveUpStructDict.ContainsKey(w.Right.Marker))
                {
                    moveUpStruct = GetMoveUpStructWithLock(w.Right.Marker);
                }
                else
                {
                    moveUpStruct = new MoveUpStruct<TKey, TValue>();
                    moveUpStruct.Gp = localArea[2].Parent;
                }
                
                moveUpStruct.Nodes.Add(localArea[0]);
                moveUpStruct.Nodes.Add(localArea[1]);
                moveUpStruct.Nodes.Add(localArea[2]);
                moveUpStruct.Nodes.Add(localArea[3]);
                moveUpStruct.Nodes.Add(localArea[4]);

                moveUpStruct.PidToIgnore.Add(myPID);
            }
            return false;
        }

        private void UpdateMoveUpStructWithLock(Guid pid, MoveUpStruct<TKey, TValue> moveUpStruct)
        {
            lock (locks[pid])
            {
                moveUpStructDict[pid] = moveUpStruct;
            }
        }

        private MoveUpStruct<TKey, TValue> GetMoveUpStructWithLock(Guid pid)
        {
            lock (locks[pid])
            {
                return moveUpStructDict[pid];
            }
        }
    }
}