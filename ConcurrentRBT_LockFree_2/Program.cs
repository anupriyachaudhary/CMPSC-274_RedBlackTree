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
            const int numOfThreads = 4;
            const int nodesPerThread = 2000000;
            const int totalNodesToInsert = numOfThreads * nodesPerThread;
            const int nodesMaxKeyValue = 16000000;
            const int searchOperationsPerThread = 1000000;

            var rbTree = new ConcurrentRBTree<long, Data>();

            //start test
            Console.WriteLine("*********** Starting Test **********");
            Console.WriteLine();
            Console.WriteLine();

            ConcurrentInsertTest(rbTree, numOfThreads, nodesPerThread, totalNodesToInsert, nodesMaxKeyValue);

            //ConcurrentSearchTest(rbTree, numOfThreads, searchOperationsPerThread, nodesMaxKeyValue);
        }

        public static void ConcurrentInsertTest(ConcurrentRBTree<long, Data> rbTree, int numOfThreads,
            long nodesPerThread, long totalNodesToInsert, long nodesMaxKeyValue)
        {
            var rand = new Random();

            var key = 1 + (long) (rand.NextDouble() * nodesMaxKeyValue);
            rbTree.Add(key, new Data {Value = key.ToString()});

            var keys = new HashSet<long>();

            for (var i = 0; i < totalNodesToInsert; i++)
            {
                long value;
                while (true)
                {
                    value = 1 + (long) (rand.NextDouble() * nodesMaxKeyValue);
                    if (!keys.Contains(value) && value != key)
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

            Console.WriteLine($"Node count after insertion: {(rbTree.Count()-1)}");
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