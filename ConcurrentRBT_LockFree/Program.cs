using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace ConcurrentRedBlackTree
{
    public class Data
    {
        public string Value { get; set; }
    }

    public class Program
    {
        static void Main(string[] args)
        {
            // Variables for search operation
            // const int numOfThreads = 12;
            // const int searchOperationsPerThread = 10000;
        
            // const int nodesPerThreadForInsert = searchOperationsPerThread * 2;
            // const long totalNodesToInsert = numOfThreads * nodesPerThreadForInsert;
            // const long nodesMaxKeyValue = totalNodesToInsert * 10;

            // Variables for mixed workload 
            const int initThreads = 12;
            const long initInsertNodesPerThread = 1000;
            const long maxKeyValue = initInsertNodesPerThread * initThreads * 3;

            const int searchWorkload = 8;
            const int insertWorkload = 2;

            var rbTree = new ConcurrentRBTree<long, Data>();

            //start test
            Console.WriteLine("*********** Starting Test **********");
            Console.WriteLine();
            Console.WriteLine();
            
            //ConcurrentInsertTest(rbTree, numOfThreads, nodesPerThreadForInsert, totalNodesToInsert, nodesMaxKeyValue);

            ConcurrentMixedWorkload(rbTree, searchWorkload, insertWorkload, initThreads, initInsertNodesPerThread, maxKeyValue);

            //ConcurrentSearchTest(rbTree, numOfThreads, searchOperationsPerThread, nodesMaxKeyValue);

            //ConcurrentDeleteTest(rbTree, numOfThreads, nodesPerThreadForDelete, totalNodesToDelete, nodesMaxKeyValue);
            
        }

        public static void ConcurrentMixedWorkload(ConcurrentRBTree<long, Data> rbTree, int searchWorkload, 
                int insertWorkload, int initThreads, long initInsertNodesPerThread, long maxKeyValue)
        {
            long initTreeSize = initThreads * initInsertNodesPerThread;
            ConcurrentInsertTest(rbTree, initThreads, initInsertNodesPerThread, initTreeSize, maxKeyValue);

            long totalNodesToInsert = initTreeSize;

            // generate valid insert items
            var count = 0;
            var keysToInsert = new HashSet<long>();
            var rand = new Random();

            while (true)
            {
                long target;
                while (true)
                {
                    target = 1 + (long)(rand.NextDouble() * maxKeyValue);
                    if (!keysToInsert.Contains(target))
                    {
                        break;
                    }
                }
                var data = rbTree.GetData(target);

                if (data == null)
                {
                    keysToInsert.Add(target);
                    count++;
                }

                if (count == totalNodesToInsert)
                {
                    break;
                }
            }
            var values = keysToInsert.Select(i => new Tuple<long, Data>(i, new Data {Value = i.ToString()})).ToArray();


            
            int numOfThreadsForInsert = insertWorkload + 1;
            var threadsForInsert = new Thread[numOfThreadsForInsert];
            long nodesPerThreadForInsert = totalNodesToInsert/numOfThreadsForInsert;

            for (var i = 0; i < numOfThreadsForInsert; i++)
            {
                var iLocal = i;
                threadsForInsert[i] = new Thread(() =>
                {
                    var start = iLocal * nodesPerThreadForInsert;
                    var end = start + nodesPerThreadForInsert - 1;
                    for (var j = start; j <= end; j++)
                    {
                        rbTree.Add(values[j].Item1, values[j].Item2);
                    }
                });

            }

            int numOfThreadsForSearch = searchWorkload + 1;
            var threadsForSearch = new Thread[numOfThreadsForSearch];
            long totalNodesToSearch = initTreeSize * (searchWorkload / insertWorkload);
            long searchOperationsPerThread = totalNodesToSearch/numOfThreadsForSearch;

            for (var i = 0; i < numOfThreadsForSearch; i++)
            {
                threadsForSearch[i] = new Thread(() =>
                {
                    rand = new Random();
                    for (var j = 0; j < searchOperationsPerThread; j++)
                    {
                        var target = 1 + (long) (rand.NextDouble() * maxKeyValue);
                        _ = rbTree.GetData(target);
                    }
                });
            }


            Console.WriteLine("************* Mixed Workload Test ***************");
            Console.WriteLine();

            Console.WriteLine($"Total search workload: {searchWorkload*10} %");
            Console.WriteLine($"Total nodes to delete: {insertWorkload*10} %");
            Console.WriteLine($"Total search operations: {totalNodesToSearch}");
            Console.WriteLine($"Total nodes inserted: {totalNodesToInsert}");
            Console.WriteLine();

            // starting inserts
            var watch = new Stopwatch();
            watch.Start();

            for (var i = 0; i < numOfThreadsForInsert; i++)
            {
                threadsForInsert[i].Start();
            }

            for (var i = 0; i < numOfThreadsForSearch; i++)
            {
                threadsForSearch[i].Start();
            }

            for (var i = 0; i < numOfThreadsForInsert; i++)
            {
                threadsForInsert[i].Join();
            }

            for (var i = 0; i < numOfThreadsForSearch; i++)
            {
                threadsForSearch[i].Join();
            }

            watch.Stop();

            Console.WriteLine($"Total time spent: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();

            Console.WriteLine($"Node count after insertion: {(rbTree.Count())}");
            Console.WriteLine();

            Console.WriteLine($"Tree depth: {rbTree.MaxDepth()}");
            Console.WriteLine();
            Console.WriteLine();
        }

        public static void ConcurrentSearchTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            long searchOperationsPerThread, long nodesMaxKeyValue)
        {
            Console.WriteLine("************* Search Test ***************");
            Console.WriteLine();

            Console.WriteLine($"Total threads: {numOfThreads}");
            Console.WriteLine($"We will perform {searchOperationsPerThread*numOfThreads} search operations on each thread");
            Console.WriteLine();

            var threads = new Thread[numOfThreads];

            for (var i = 0; i < numOfThreads; i++)
            {
                threads[i] = new Thread(() =>
                {
                    var rand = new Random();
                    for (var j = 0; j < searchOperationsPerThread; j++)
                    {
                        var target = 1 + (long) (rand.NextDouble() * nodesMaxKeyValue);
                        _ = rbTree.GetData(target);
                    }
                });
            }

            // search test
            var watch = new Stopwatch();
            watch.Start();

            foreach (var thread in threads)
            {
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }

            watch.Stop();

            Console.WriteLine($"Total time spent in search: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();
            Console.WriteLine();
        }

        public static void ConcurrentDeleteTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            int nodesPerThread, int totalNodesToDelete, long nodesMaxKeyValue)
        {
            if (rbTree.isValidRBT(nodesMaxKeyValue + 1) == false)
            {
                Console.WriteLine($"After insertion, RBT is invalid");
                Debug.Assert(!rbTree.isValidRBT(nodesMaxKeyValue + 1));
            }

            //rbTree.printLevelOrder();

            Console.WriteLine("************* Delete Test ***************");
            Console.WriteLine();

            var keysToDelete = GenerateItemsFromTree(rbTree, totalNodesToDelete, nodesMaxKeyValue).ToArray();
            
            var threads = new Thread[numOfThreads];

            for (var i = 0; i < threads.Length; i++)
            {
                var iLocal = i;
                threads[i] = new Thread(() =>
                {
                    var start = iLocal * nodesPerThread;
                    var end = start + nodesPerThread - 1;
                    for (var j = start; j <= end; j++)
                    {
                        rbTree.Remove(keysToDelete[j]);
                    }
                });
                threads[i].Name = i.ToString();
            }

            Console.WriteLine($"Total nodes to delete: {totalNodesToDelete}");
            Console.WriteLine($"Total threads: {numOfThreads}");
            Console.WriteLine($"Total nodes per thread: {nodesPerThread}");
            Console.WriteLine();

            // starting deletes
            var watch = new Stopwatch();
            watch.Start();

            foreach (var thread in threads)
            {
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }

            watch.Stop();

            if (rbTree.isValidRBT(nodesMaxKeyValue + 1) == false)
            {
                Console.WriteLine($"After delete, RBT is invalid");
            }

            Console.WriteLine($"Total time spent in deletion: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();

            Console.WriteLine($"Node count after deletion: {(rbTree.Count())}");
            Console.WriteLine();

            Console.WriteLine($"Tree depth: {rbTree.MaxDepth()}");
            Console.WriteLine();
            Console.WriteLine();
        }

        
        public static void ConcurrentInsertTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            long nodesPerThread, long totalNodesToInsert, long nodesMaxKeyValue)
        {
            var rand = new Random();

            var keys = new HashSet<long>();

            for (var i = 0; i < totalNodesToInsert; i++)
            {
                long value;
                while (true)
                {
                    value = 1 + (long) (rand.NextDouble() * nodesMaxKeyValue);
                    if (!keys.Contains(value))
                    {
                        break;
                    }
                }

                keys.Add(value);
            }

            var values = keys.Select(i => new Tuple<long, Data>(i, new Data {Value = i.ToString()})).ToArray();

            var threads = new Thread[numOfThreads];

            for (var i = 0; i < threads.Length; i++)
            {
                var iLocal = i;
                threads[i] = new Thread(() =>
                {
                    var start = iLocal * nodesPerThread;
                    var end = start + nodesPerThread - 1;
                    for (var j = start; j <= end; j++)
                    {
                        rbTree.Add(values[j].Item1, values[j].Item2);
                    }
                });
                threads[i].Name = i.ToString();
            }

            Console.WriteLine("************* Insertion Test ***************");
            Console.WriteLine();

            Console.WriteLine($"Total nodes to insert: {totalNodesToInsert}");
            Console.WriteLine($"Total threads: {numOfThreads}");
            Console.WriteLine($"Total nodes per thread: {nodesPerThread}");
            Console.WriteLine();

            // starting inserts
            var watch = new Stopwatch();
            watch.Start();

            foreach (var thread in threads)
            {
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }

            watch.Stop();

            Console.WriteLine($"Total time spent in insertion: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();

            Console.WriteLine($"Node count after insertion: {(rbTree.Count())}");
            Console.WriteLine();

            Console.WriteLine($"Tree depth: {rbTree.MaxDepth()}");
            Console.WriteLine();
            Console.WriteLine();

            return;
        }

        public static HashSet<long> GenerateItemsFromTree(ConcurrentRBTree<long, Data> rbTree, long numOfItems, long nodesMaxKeyValue)
        {
            // generate valid deletable items
            var count = 0;
            var searchItems = new HashSet<long>();
            var rand = new Random();

            while (true)
            {
                long target;
                while (true)
                {
                    target = 1 + (long)(rand.NextDouble() * nodesMaxKeyValue);
                    if (!searchItems.Contains(target))
                    {
                        break;
                    }
                }
                var data = rbTree.GetData(target);

                if (data != null)
                {
                    searchItems.Add(data.Item1);
                    count++;
                }

                if (count == numOfItems)
                {
                    break;
                }
            }

            return searchItems;
        }
    }
}

