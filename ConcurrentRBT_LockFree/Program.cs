﻿using System;
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
            // // variables for insert
            // const int numOfThreads = 12;
            // const long nodesPerThread = 10000;
            // const long totalNodesToInsert = numOfThreads * nodesPerThread;
            // const long nodesMaxKeyValue = 10000000;

            // Variables for delete
            // const int numOfThreads = 64;
            // const int nodesPerThread = 100;
            // const int totalNodesToDelete = numOfThreads * nodesPerThread;
            // const long totalNodesToInsert = totalNodesToDelete * 4;
            // const long nodesMaxKeyValue = totalNodesToInsert * 10;
            
            // // Variables for search operation
            // const int numOfThreads = 12;
            // const int searchOperationsPerThread = 10000;
            // const long nodesMaxKeyValue = 10000000;

            // Variables for search operation
            const int numOfThreads = 1;
            const int nodesPerThreadForSearch = 10000;
            const int totalNodesToSearch = numOfThreads * nodesPerThreadForSearch;
            const int nodesPerThreadForInsert = nodesPerThreadForSearch * 2;
            const long totalNodesToInsert = numOfThreads * nodesPerThreadForInsert;
            const long nodesMaxKeyValue = totalNodesToInsert * 10;

            var rbTree = new ConcurrentRBTree<long, Data>();

            //start test
            Console.WriteLine("*********** Starting Test **********");
            Console.WriteLine();
            Console.WriteLine();

            //SimpleInsertDeleteTest(rbTree, totalNodesToDelete, totalNodesToInsert, nodesMaxKeyValue);
            
            ConcurrentInsertTest(rbTree, numOfThreads, nodesPerThreadForInsert, totalNodesToInsert, nodesMaxKeyValue);

            ConcurrentSearchTest(rbTree, numOfThreads, nodesPerThreadForSearch, totalNodesToSearch, nodesMaxKeyValue);

            //ConcurrentDeleteTest(rbTree, numOfThreads, nodesPerThread, totalNodesToDelete, nodesMaxKeyValue);
            
            //ConcurrentSearchTest(rbTree, numOfThreads, searchOperationsPerThread, nodesMaxKeyValue);
        }

        public static void ConcurrentSearchTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            int nodesPerThread, int totalNodesToSearch, long nodesMaxKeyValue)
        {
            Console.WriteLine("************* Search Test ***************");
            Console.WriteLine();

            Console.WriteLine($"We will perform {totalNodesToSearch} search operations");
            Console.WriteLine();

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

                if (count == totalNodesToSearch)
                {
                    break;
                }
            }
            var keysToSearch = searchItems.ToArray();
            
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
                        rbTree.Search(keysToSearch[j]);
                    }
                });
                threads[i].Name = i.ToString();
            }

            Console.WriteLine($"Total search operations: {totalNodesToSearch}");
            Console.WriteLine($"Total threads: {numOfThreads}");
            Console.WriteLine($"Total search operations per thread: {nodesPerThread}");
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

            Console.WriteLine($"Total time spent in search operations: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();
            Console.WriteLine();
        }

        public static void SearchTest(ConcurrentRBTree<long, Data> rbTree, int totalNodesToSearch, long nodesMaxKeyValue)
        {
            Console.WriteLine("************* Search Test ***************");
            Console.WriteLine();

            Console.WriteLine($"We will perform {totalNodesToSearch} search operations");
            Console.WriteLine();

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

                if (count == totalNodesToSearch)
                {
                    break;
                }
            }
            var keysToSearch = searchItems.ToArray();

            for (var i = 0; i < totalNodesToSearch; i++)
            {
                rbTree.Search(keysToSearch[i]);
            }

            Console.WriteLine($"Total search operations: {totalNodesToSearch}");
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

            Console.WriteLine($"We will perform {totalNodesToDelete} delete operations");
            Console.WriteLine();

             // generate valid deletable items
            var count = 0;
            var deleteItems = new HashSet<long>();
            var rand = new Random();

            while (true)
            {
                long target;
                while (true)
                {
                    target = 1 + (long)(rand.NextDouble() * nodesMaxKeyValue);
                    if (!deleteItems.Contains(target))
                    {
                        break;
                    }
                }
                var data = rbTree.GetData(target);

                if (data != null)
                {
                    deleteItems.Add(data.Item1);
                    count++;
                }

                if (count == totalNodesToDelete)
                {
                    break;
                }
            }
            var keysToDelete = deleteItems.ToArray();

            /* Console.WriteLine("Deleted keys");
            foreach (var item in keysToDelete)
            {
                Console.WriteLine(item);
            }*/
            
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
                        //Console.WriteLine($"Key = {keysToDelete[j]} is {rbTree.Remove(keysToDelete[j])}");
                        rbTree.Remove(keysToDelete[j]);
                        //if (j%10 == 1)
                        // {
                        //     Console.WriteLine($"Checking validity at node no. {j-start}");
                        //     if (rbTree.isValidRBT(nodesMaxKeyValue) == false)
                        //     {
                        //         Console.WriteLine($"After deleting Key = {keysToDelete[j]}, RBT is invalid");
                        //     }
                        // }
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

            //var data = new long[] {86, 122, 21, 160, 98, 25, 154, 62, 138, 119, 89, 87, 130, 141, 120, 67};
            //values = data.Select(i => new Tuple<long, Data>(i, new Data {Value = i.ToString()})).ToArray();

            //values.ToList().ForEach(m => Console.Write($"{m.Item1}, "));
            //Console.WriteLine("\n\n");

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
                        //Console.WriteLine($"{DateTime.UtcNow.ToFileTimeUtc()},{Thread.CurrentThread.ManagedThreadId},{values[j].Item1}");
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

        public static void ConcurrentInsert(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            long nodesPerThread, HashSet<long> keys)
        {
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
                        //Console.WriteLine($"Key = {values[j].Item1} is inserted!");
                    }
                });
            }

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

            Console.WriteLine($"Node count after insertion: {(rbTree.Count())}");
            Console.WriteLine($"Tree depth: {rbTree.MaxDepth()}");
            Console.WriteLine();
            Console.WriteLine();
        }

        public static void SimpleInsertDeleteTest(ConcurrentRBTree<long, Data> rbTree, int totalNodesToDelete, 
            long totalNodesToInsert, long nodesMaxKeyValue)
        {
            Console.WriteLine("************* Red Black Tree ***************");

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

            var set = string.Join(", ", keys);
            Console.WriteLine(set);
            
            ConcurrentInsert(rbTree, 1, totalNodesToInsert, keys);


            Console.WriteLine("************* Deletion Test ***************");
            Console.WriteLine();

            var randomKeys = keys.OrderBy(x => rand.Next()).Take(totalNodesToDelete);
            List<long> keysToDelete = randomKeys.ToList();

            set = string.Join(", ", keysToDelete);
            Console.WriteLine(set);

            foreach(var k in keysToDelete)
            {
                Console.WriteLine($"Key to be deleted: {k}");
                rbTree.Remove(k);
            }

            Console.WriteLine($"Node count after deletion: {(rbTree.Count())}");
            Console.WriteLine();
        }

        public static void ConcurrentSearchTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            long searchOperationsPerThread, long nodesMaxKeyValue)
        {
            Console.WriteLine("************* Search Test ***************");
            Console.WriteLine();

            Console.WriteLine($"Total threads: {numOfThreads}");
            Console.WriteLine($"We will perform {searchOperationsPerThread} search operations on each thread");
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
    }
}

// var randomKeys = keys.OrderBy(x => rand.Next()).Take(totalNodesToDelete);
// List<long> keysToDelete = randomKeys.ToList();