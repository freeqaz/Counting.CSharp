using NetMQ;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Counting.Ventilator
{
    public class Program
    {
        // This isn't used, but this would be the way you could run instances
        // Without using the testharness. Could be useful if you wanted to run
        // The service over multiple machines.
        public static void Main(string[] args)
        {
            TaskVentilator vent = new TaskVentilator(args[0], args[1]);
        }
    }

    public class TaskVentilator
    {
        string _senderAddress;

        string _sinkAddress;

        public bool IsWorking = true;

        public TaskVentilator(string senderAddress, string sinkAddress)
        {
            _senderAddress = senderAddress;

            _sinkAddress = sinkAddress;
        }

        public void DoWork()
        {

            using (var context = NetMQContext.Create())
            {
                using (NetMQSocket sender = context.CreatePushSocket(),
                                   sink   = context.CreatePushSocket())
                {
                    sink.Bind(_sinkAddress);

                    sender.Bind(_senderAddress);

                    // Wait for the workers to do their TCP thing.
                    Thread.Sleep(100);

                    Console.WriteLine("Sending tasks to workers…");

                    var randomizer = new Random(DateTime.Now.Millisecond);

                    // This is the size per 'batch'.
                    // Using this emulate reading from a file too large to fit in memory.
                    const int batchSize = 268;

                    // Set this to 200000 to simulate ~100gb of data.
                    // Current set to ~10gb of data.
                    const int numberOfBatches = 20000;

                    // 100gb = 107374182400 bytes.
                    // 100gb / 20 = 53687091 words in file
                    // 53687091 / 200000 = ~268 words per 'batch'.
                    // That's around 50kb per chunk. Seems reasonable.
                    // Using TCP right now, could use inproc too.

                    int currentBatch = 0;
                    while (currentBatch < numberOfBatches)
                    {
                        // Just putting it all in a big string that's sent over ZeroMQ.
                        // Separated by commas, but it's arbitrary.
                        StringBuilder workToSend = new StringBuilder();

                        workToSend.Append(string.Format("#{0},", currentBatch));

                        // Random 'word' to send workers
                        for (int i = 0; i < batchSize; i++)
                        {
                            // We can tweak the number of unique keys here...
                            // More unique keys is more expensive to write to disk.
                            // Anything greater than 3020000 results in huuuuge flushes to
                            // The disk and gets pretty slow.
                            // Anything less than that can remain in memory (with 4gb).
                            // Best solution: Virtual Memory. You can keep pointers in memory
                            // And not have manually handle flushing to the disk yourself.
                            workToSend.Append(randomizer.Next(1, 5000000) + ",");
                        }

                        // Print out some output to show we're working.
                        if (currentBatch % 10000 == 0)
                        {
                            Console.WriteLine("{0}% Complete pushing work", ((double)currentBatch / (double)numberOfBatches * 100).ToString("0.0"));
                        }

                        sink.Send("#" + currentBatch);
                        sender.Send(workToSend.ToString().TrimEnd(','));

                        currentBatch++;
                    }

                    Console.WriteLine("Done with queueing every job! Sending done to Sink.");

                    sink.Send("done");

                    IsWorking = false;
                }
            }
        }
    }
}
