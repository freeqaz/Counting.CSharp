using Counting.Shared;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using NetMQ;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Counting.Sink
{
    public class Program
    {
        // This isn't used, but this would be the way you could run instances
        // Without using the testharness. Could be useful if you wanted to run
        // The service over multiple machines.
        public static void Main(string[] args)
        {
            TaskSink sink = new TaskSink(args[0], args[1]);
        }
    }

    public class TaskSink
    {
        private string _receiverAddress;

        public string _ventilatorReceiveAddress;

        public bool WaitingOnWork = true;
        private bool _writingData = true;

        private List<string> _processedJobs;

        private List<string> _knownJobs;

        public TaskSink(string receiverAddress, string ventilatorReceiveAddress)
        {
            _receiverAddress = receiverAddress;

            _ventilatorReceiveAddress = ventilatorReceiveAddress;

            _processedJobs = new List<string>();
            _knownJobs = new List<string>();
        }

        public void DoWork()
        {
            BPlusTree<string, int>.Options options = new BPlusTree<string, int>.Options(
                PrimitiveSerializer.String, PrimitiveSerializer.Int32, StringComparer.Ordinal)
            {
                CreateFile = CreatePolicy.Always, // Always overwrite
                FileName = @"Storage.dat"
            };

            using (BPlusTree<string, int> map = new BPlusTree<string, int>(options))
            {
                ProcessDataFromWorkers(map);
            }
        }

        private void ProcessDataFromWorkers(BPlusTree<string, int> map)
        {
            
                
            using (var context = NetMQContext.Create())
            {

                using (NetMQSocket receiver = context.CreatePullSocket())
                {
                    Task.Run(() =>
                    {
                        // The Sink listens to the Ventilator for news about jobs
                        using (NetMQSocket sink = context.CreatePullSocket())
                        {
                            sink.Connect(_ventilatorReceiveAddress);

                            while (_writingData)
                            {
                                string message = "";
                                try
                                {
                                    message = sink.ReceiveString();
                                }
                                catch (ObjectDisposedException e)
                                {
                                    // We're done. Exit the loop.
                                    break;
                                }

                                // A new job was pushed
                                if (message != "done")
                                {
                                    _knownJobs.Add(message);
                                }
                                // We're done!
                                else
                                {
                                    Task.Run(() =>
                                    {
                                        // While the count isn't the same, wait.
                                        // Todo: Bake better status monitoring to prevent deadlocking
                                        //       if things are ever dropped.
                                        while (_processedJobs.Count != _knownJobs.Count)
                                        {
                                            Thread.Sleep(10);
                                        }
                                        receiver.Close();
                                        sink.Close();
                                        _writingData = false;

                                    });
                                }
                            }
                        }
                    });

                    receiver.Bind(_receiverAddress);

                    var dict = new Dictionary<string, int>();

                    while (_writingData)
                    {
                        string message = "";
                        try
                        {
                            message = receiver.ReceiveString();
                        }
                        catch (ObjectDisposedException e)
                        {
                            // We know there isn't more work, so we break this loop.
                            break;
                        }

                        var jobResult = JsonConvert.DeserializeObject<JobResult>(message);

                        foreach (var word in jobResult.Data)
                        {
                            // Update existing values
                            if (dict.ContainsKey(word.Key))
                            {
                                dict[word.Key] = word.Value + dict[word.Key];
                            }
                            else // Add new value
                            {
                                dict[word.Key] = word.Value;
                            }

                            // This is around the max that we can hold in 4gb safely.
                            // Assuming our strings aren't suuuper long.
                            if (dict.Count > 3020000)
                            {
                                Console.WriteLine("Flushing " + jobResult.JobId + "... ");

                                var stopwatch = new Stopwatch();
                                stopwatch.Start();

                                FlushToDisk(map, dict);

                                dict = new Dictionary<string, int>();

                                stopwatch.Stop();

                                Console.WriteLine("Done flushing " + jobResult.JobId + ". Time: " + stopwatch.ElapsedMilliseconds + "ms");
                            }
                        }

                        _processedJobs.Add(jobResult.JobId);
                    }

                    Console.WriteLine("Flattening data to TSV file.");

                    // Save our changes to the ondisk db
                    if (map.Any())
                    {
                        Console.WriteLine("Pushing data to BPlusTree.");
                        FlushToDisk(map, dict);

                        Console.WriteLine(map.Count);

                        WriteToTSVFile(map, "Output.tsv");
                    }
                    // Everything is in memory so just use that.
                    else
                    {
                        WriteToTSVFile(dict, "Output.tsv");
                    }

                    // We're done! 
                    WaitingOnWork = false;
                }

            }
        }

        private static void FlushToDisk(BPlusTree<string, int> map, Dictionary<string, int> dict)
        {
            int takenCount = 0;
            while (takenCount < dict.Count)
            {
                int count = 0;

                var keysToUpdate = new List<Tuple<string, int>>();

                // Only update in blocks of 10000 because we don't want to
                // Pop our max memory when calculating diffs.
                int amountToTake = 10000;

                // We might not be able to take exactly 10000.
                if (dict.Count - count < 10000)
                {
                    amountToTake = dict.Count - count;
                    count = 0;
                }

                // Create list of diffs
                foreach (var kvp in dict.Skip(takenCount).Take(amountToTake))
                {
                    int value;
                    if (map.TryGetValue(kvp.Key, out value))
                    {
                        keysToUpdate.Add(new Tuple<string, int>(kvp.Key, kvp.Value + value));
                    }
                }

                // Apply diffs (can't modify dictionary during enumeration)
                foreach (var update in keysToUpdate)
                {
                    dict[update.Item1] = update.Item2;
                }

                count += amountToTake;
                takenCount += amountToTake;
            }

            BulkInsertOptions bio = new BulkInsertOptions();
            //bio.ReplaceContents = true;
            bio.DuplicateHandling = DuplicateHandling.LastValueWins;

            // BPlusTree is optimized to insert in batches.
            map.BulkInsert(dict, bio);
        }

        private void WriteToTSVFile(IDictionary<string, int> data, string filename)
        {
            try
            {
                var streamWriter = new StreamWriter(File.Create(filename));

                foreach (var entry in data)
                {
                    streamWriter.WriteLine(entry.Key + "\t" + entry.Value);
                }

                streamWriter.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Something went wrong writing to disk. :/");
            }
        }
    }
}
