using Counting.Sink;
using Counting.Ventilator;
using Counting.Worker;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

// This app is loosely based around the Parallel Pipeline Ventilator
// From the ZeroMQ ZGuide.
// http://zguide.zeromq.org/page:all

namespace Counting.TestHarness
{
    public class TestHarness
    {
        static void Main(string[] args)
        {
            // Create the Ventilator object.
            var ventilator = new TaskVentilator("tcp://localhost:5557", "tcp://localhost:9001");
            Thread ventilatorThread = new Thread(ventilator.DoWork);

            var taskWorkers = new List<TaskWorker>();
            var workerThreads = new List<Thread>();

            // Create some worker objects.
            for (int i = 0; i < 4; i++)
            {
                var worker = new TaskWorker("tcp://localhost:5557", "tcp://localhost:5558");
                Thread workerThread = new Thread(worker.DoWork);
                taskWorkers.Add(worker);
                workerThreads.Add(workerThread);
            }

            // Create the Sink object.
            var sink = new TaskSink("tcp://localhost:5558", "tcp://localhost:9001");
            Thread sinkThread = new Thread(sink.DoWork);

            // Start the Worker threads.
            int workerCount = 0;
            foreach (var workerThread in workerThreads)
            {
                workerThread.Start();
                Console.WriteLine("Main thread: Starting Working thread #{0}.", workerCount);
                workerCount++;
            }

            // Start the Sink thread.
            sinkThread.Start();
            Console.WriteLine("Main thread: Starting Sink thread.");

            // Start the Ventilator thread.
            ventilatorThread.Start();
            Console.WriteLine("Main thread: Starting Ventilator thread.");

            while (ventilator.IsWorking)
            {
                Thread.Sleep(1000);
            }

            Console.WriteLine("Waiting for disk!");

            while (sink.WaitingOnWork)
            {
                Thread.Sleep(1000);
            }

            Console.WriteLine("We're all done!");

            Console.ReadLine();
        }
    }
}