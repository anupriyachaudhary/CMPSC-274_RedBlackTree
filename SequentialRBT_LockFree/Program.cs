using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;

namespace SequentialRBTree
{
    public class Data
    {
        public string Value { get; set; }
    }

    public class Program
    {
        static void Main(string[] args)
        {
            // read command line parameters
            const int totalNodesToDelete = 100000;
            const int totalNodesToInsert = totalNodesToDelete * 4;
            const int nodesMaxKeyValue = totalNodesToInsert * 10;
            //const int searchOperations = 1000000;

            var rbTree = new SequentialRBTree<long, Data>();

            //start test
            Console.WriteLine("*********** Starting Test **********");
            Console.WriteLine();
            Console.WriteLine();

            HashSet<long> keys = InsertTest(rbTree, totalNodesToInsert, nodesMaxKeyValue);

            //SearchTest(rbTree, searchOperations, nodesMaxKeyValue);

            DeleteTest(rbTree, totalNodesToDelete, nodesMaxKeyValue, keys);
        }

        public static HashSet<long> InsertTest(SequentialRBTree<long, Data> rbTree, int totalNodesToInsert, int nodesMaxKeyValue)
        {
            // generate input data
            var rand = new Random();
            var keys = new HashSet<long>();

            for (var i = 0; i < totalNodesToInsert; i++)
            {
                long value;
                while (true)
                {
                    value = 1 + (long)(rand.NextDouble() * nodesMaxKeyValue);
                    if (!keys.Contains(value))
                    {
                        break;
                    }
                }
                keys.Add(value);
            }

            var values = keys.Select(i => new Tuple<long, Data>(i, new Data { Value = i.ToString() }));

            Console.WriteLine("************* Insertion Test ***************");
            Console.WriteLine();

            Console.WriteLine($"Total nodes to insert: {totalNodesToInsert}");
            Console.WriteLine();

            // starting inserts
            var watch = new Stopwatch();
            watch.Start();

            foreach (var value in values)
            {
                rbTree.Add(value.Item1, value.Item2);
            }

            watch.Stop();

            Console.WriteLine($"Total time spent in insertion: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();

            Console.WriteLine($"Node count after insertion: {rbTree.Count()}");
            Console.WriteLine();

            Console.WriteLine($"Tree depth: {rbTree.MaxDepth()}");
            Console.WriteLine();
            Console.WriteLine();

            return keys;
        }

        public static void SearchTest(SequentialRBTree<long, Data> rbTree, int searchOperations, int nodesMaxKeyValue)
        {
            Console.WriteLine("************* Search Test ***************");
            Console.WriteLine();

            Console.WriteLine($"We will perform {searchOperations} search operations");
            Console.WriteLine();

            // search test
            var watch = new Stopwatch();
            watch.Start();

            var rand = new Random();
            for (var i = 0; i < searchOperations; i++)
            {
                var target = 1 + (long)(rand.NextDouble() * nodesMaxKeyValue);

                _ = rbTree.GetData(target);
            }

            watch.Stop();

            Console.WriteLine($"Total time spent in search: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();
            Console.WriteLine();
        }

        public static void DeleteTest(SequentialRBTree<long, Data> rbTree, int totalNodesToDelete, int nodesMaxKeyValue, HashSet<long> keys)
        {
            Console.WriteLine("************* Delete Test ***************");
            Console.WriteLine();

            Console.WriteLine($"We will perform {totalNodesToDelete} delete operations");
            Console.WriteLine();

            // generate valid deletable items
            var rand = new Random();
            var randomKeys = keys.OrderBy(x => rand.Next()).Take(totalNodesToDelete);
            List<long> keysToDelete = randomKeys.ToList();

            // delete test
            var watch = new Stopwatch();
            watch.Start();

            foreach (var key in keysToDelete)
            {
                rbTree.Remove(key);
            }

            watch.Stop();

            Console.WriteLine($"Total time spent in deletion: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine();

            Console.WriteLine($"Node count after deletion: {rbTree.Count()}");
            Console.WriteLine();
            Console.WriteLine();
        }
    }
}
