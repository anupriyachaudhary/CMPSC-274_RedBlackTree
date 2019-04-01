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
            // // variables for insert
            // const int numOfThreads = 12;
            // const long nodesPerThread = 10000;
            // const long totalNodesToInsert = numOfThreads * nodesPerThread;
            // const long nodesMaxKeyValue = 10000000;

            // Variables for delete
            const int numOfThreads = 1;
            const int nodesPerThread = 20;
            const int totalNodesToDelete = numOfThreads * nodesPerThread;
            const long totalNodesToInsert = totalNodesToDelete * 4;
            const long nodesMaxKeyValue = totalNodesToInsert * 10;
            
            // // Variables for search operation
            // const int numOfThreads = 12;
            // const int searchOperationsPerThread = 10000;
            // const long nodesMaxKeyValue = 10000000;

            var rbTree = new ConcurrentRBTree<long, Data>();

            //start test
            Console.WriteLine("*********** Starting Test **********");
            Console.WriteLine();
            Console.WriteLine();

            //SimpleInsertDeleteTest(rbTree, totalNodesToDelete, totalNodesToInsert, nodesMaxKeyValue);
            HashSet<long> keys = ConcurrentInsertTest(rbTree, numOfThreads, nodesPerThread * 4, totalNodesToInsert, nodesMaxKeyValue);
            ConcurrentDeleteTest(rbTree, numOfThreads, nodesPerThread, totalNodesToDelete, keys);
            // ConcurrentSearchTest(rbTree, numOfThreads, searchOperationsPerThread, nodesMaxKeyValue);
        }

        public static void ConcurrentDeleteTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            int nodesPerThread, int totalNodesToDelete, HashSet<long> keys)
        {
            Console.WriteLine("************* Delete Test ***************");
            Console.WriteLine();

            Console.WriteLine($"We will perform {totalNodesToDelete} delete operations");
            Console.WriteLine();

            // generate valid deletable items
            var rand = new Random();
            var randomKeys = keys.OrderBy(x => rand.Next()).Take(totalNodesToDelete);
            //List<long> keysToDelete = randomKeys.ToList();
            List<long> keysToDelete = new List<long>(){45, 348, 294, 287, 280, 570, 107, 679, 105, 332, 180, 708, 194, 389, 213, 234, 363, 539, 262, 63};

            var set = string.Join(", ", keysToDelete);
            Console.WriteLine(set);

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
                        Console.WriteLine($"Key = {keysToDelete[j]} is {rbTree.Remove(keysToDelete[j])}");
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

            Console.WriteLine($"Total time spent in deletion: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();

            Console.WriteLine($"Node count after deletion: {(rbTree.Count())}");
            Console.WriteLine();

            Console.WriteLine($"Tree depth: {rbTree.MaxDepth()}");
            Console.WriteLine();
            Console.WriteLine();
        }

        
        public static HashSet<long> ConcurrentInsertTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            long nodesPerThread, long totalNodesToInsert, long nodesMaxKeyValue)
        {
            var rand = new Random();

            var keys = new HashSet<long>(){470, 539, 634, 467, 571, 113, 375, 207, 180, 213, 52, 343, 744, 27, 503, 43, 596, 121, 175, 234, 780, 363, 63, 582, 443, 122, 391, 611, 728, 115, 7, 348, 304, 262, 742, 289, 240, 308, 570, 401, 332, 292, 184, 389, 473, 287, 515, 403, 708, 294, 18, 139, 107, 476, 25, 450, 523, 564, 280, 66, 622, 45, 796, 440, 565, 562, 554, 356, 614, 406, 633, 448, 72, 679, 194, 770, 1, 105, 97, 326};

            // for (var i = 0; i < totalNodesToInsert; i++)
            // {
            //     long value;
            //     while (true)
            //     {
            //         value = 1 + (long) (rand.NextDouble() * nodesMaxKeyValue);
            //         if (!keys.Contains(value))
            //         {
            //             break;
            //         }
            //     }

            //     keys.Add(value);
            // }

            var set = string.Join(", ", keys);
            Console.WriteLine(set);

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
                //threads[i].Name = i.ToString();
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

            return keys;
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