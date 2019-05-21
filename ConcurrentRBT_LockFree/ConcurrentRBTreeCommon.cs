using System;
using System.Collections.Generic;
using System.Linq;

namespace ConcurrentRedBlackTree
{
    public partial class ConcurrentRBTree<TKey, TValue>
        where TValue : class
        where TKey : IComparable<TKey>, IComparable, IEquatable<TKey>
    {
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
            if (node.IsSentinel)
                return true;

            bool isLow = false;
            bool isHigh = false;
            if (node.Key.CompareTo(low) > 0)
            {
                isLow = true;
            }
            if (node.Key.CompareTo(high) < 0)
            {
                isHigh = true;
            }
            if (isLow == false || isHigh == false)
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
            for (i = 1; i <= height; i++)
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
                printGivenLevel(node.Left, level - 1);
                printGivenLevel(node.Right, level - 1);
            }
        }

        private void ReleaseFlagsAfterSuccess(IEnumerable<RedBlackNode<TKey, TValue>> nodesToRelease, Guid pid)
        {
            foreach (var nd in nodesToRelease)
            {
                if (!IsIn(nd, pid))
                {
                    nd.FreeNodeAtomically();
                }
                //nd is in inherited local area
                else
                {
                    if (IsGoalNode(nd, pid))
                    {
                        // release unneeded flags in moveUpStruct and discard moveUpStruct
                        foreach (var moveUpStruct in moveUpStructDict[pid])
                        {
                            foreach (var node in moveUpStruct.Nodes)
                            {
                                node?.FreeNodeAtomically();
                            }
                        }
                        moveUpStructDict.TryRemove(pid, out var _);
                    }
                }
            }
        }

        private void ReleaseFlagsAfterFailure(IEnumerable<RedBlackNode<TKey, TValue>> nodesToRelease, Guid pid)
        {
            foreach (var nd in nodesToRelease)
            {
                if (!IsIn(nd, pid))
                {
                    nd.FreeNodeAtomically();
                }
            }
        }

        private bool IsIn(RedBlackNode<TKey, TValue> node, Guid pid)
        {
            if(!moveUpStructDict.ContainsKey(pid))
            {
                return false;
            }

            return moveUpStructDict[pid].Last().Nodes.Any(m => m == node);
        }

        private bool IsGoalNode(RedBlackNode<TKey, TValue> nodeToRelease, Guid pid)
        {
            if(!moveUpStructDict.ContainsKey(pid))
            {
                return false;
            }

            var moveUpStruct = moveUpStructDict[pid].Last();

            while(true)
            {
                if(ConcurrentRBTreeHelper.OccupyAndCheck<TKey, TValue>(() => moveUpStruct.Nodes[2].Parent))
                {
                    break;
                }
            }

            return nodeToRelease == moveUpStruct.Nodes[2].Parent;
        }

        private Guid GetPIDtoIgnore(Guid pid)
        {
            if(!moveUpStructDict.ContainsKey(pid))
            {
                return Guid.Empty;
            }
            return moveUpStructDict[pid].Last().PidToIgnore;
        }

        private bool IsTooCloseProcess(Guid pid)
        {
            return moveUpStructDict.ContainsKey(pid);
        }

        private bool IsSpacingRuleSatisfied(
            RedBlackNode<TKey, TValue> t,
            Guid pid,
            bool isParentOccupied,
            RedBlackNode<TKey, TValue> z = null)
        {
            // we already hold flags on both t and z.
            // check that t has no marker set
            if (z == null || z != t)
            {
                if (IsMarkerSet(t.Marker))
                {
                    return false;
                }
            }

            // check that t's parent has no flag or marker
            RedBlackNode<TKey, TValue> tp = t.Parent;

            if (z == null || z != tp)
            {
                if (!isParentOccupied)
                {
                    if(!ConcurrentRBTreeHelper.OccupyAndCheck<TKey, TValue>(() => t.Parent, () => !IsIn(t.Parent, pid)))
                    {
                        return false;
                    }
                }

                if (IsMarkerSet(tp.Marker))
                {
                    if (!isParentOccupied)
                    {
                        tp.FreeNodeAtomically();
                    }
                    return false;
                }
            }

            //check that t's sibling has no flag or marker or PIDtoIgnore
            if (!ConcurrentRBTreeHelper.OccupyAndCheck<TKey, TValue>(() => t == tp.Left ? tp.Right : tp.Left,
                () => !IsIn(t == tp.Left ? tp.Right : tp.Left, pid),
                () =>
                {
                    if ((z == null || z != tp) && !isParentOccupied)
                    {
                        ReleaseFlagsAfterFailure(new []{ tp }, pid);
                    }
                }))
            {
                return false;
            }

            var ts = t == tp.Left ? tp.Right : tp.Left;
            var PIDtoIgnore = GetPIDtoIgnore(pid);
            bool isMarkerAllowed = true;
            if (IsMarkerSet(ts.Marker) && (!IsTooCloseProcess(pid) || ts.Marker != PIDtoIgnore))
            {
                isMarkerAllowed = false;
            }

            // release flags on ts and tp
            var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();
            nodesToRelease.Add(ts);
            if (z == null || z != tp)
            {
                if (!isParentOccupied)
                {
                    nodesToRelease.Add(tp);
                }
            }
            ReleaseFlagsAfterFailure(nodesToRelease, pid);

            return isMarkerAllowed;
        }

        private bool IsMarkerSet(Guid marker)
        {
            return marker != Guid.Empty;
        }

        private bool SetMarker(
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
                bool IsSetMarkerSuccess = SetMarker(markerPositions.ToList(), pid, z);

                // release flags on four nodes after putting markers
                ReleaseFlagsAfterFailure(markerPositions, pid);

                return IsSetMarkerSuccess;
            }

            var nodesToRelease = new List<RedBlackNode<TKey, TValue>>();

            // get additional marker(s) above
            RedBlackNode<TKey, TValue> firstnew = markerPositions[3].Parent;

            if (!IsIn(firstnew, pid) && !firstnew.OccupyNodeAtomically())
            {
                nodesToRelease.AddRange(markerPositions);
                ReleaseFlagsAfterFailure(nodesToRelease, pid);
                return false;
            }
            if (firstnew != markerPositions[3].Parent && !IsSpacingRuleSatisfied(firstnew, pid, false, null))
            {
                nodesToRelease.AddRange(markerPositions);
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

                return true; ;
            }

            if (numAdditional == 2)
            {
                // get additional marker above
                RedBlackNode<TKey, TValue> secondnew = firstnew.Parent;

                if (!IsIn(secondnew, pid) && !secondnew.OccupyNodeAtomically())
                {
                    nodesToRelease.AddRange(markerPositions);
                    nodesToRelease.Add(firstnew);
                    ReleaseFlagsAfterFailure(nodesToRelease, pid);
                    return false;
                }
                if (secondnew != firstnew.Parent && !IsSpacingRuleSatisfied(secondnew, pid, false, null))
                {
                    nodesToRelease.AddRange(markerPositions);
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

                return true; ;
            }

            return true;
        }
    }
}
