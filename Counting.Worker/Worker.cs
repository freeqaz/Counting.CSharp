using Counting.Shared;
using NetMQ;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Counting.Worker
{
    public class Program
    {
        // This isn't used, but this would be the way you could run instances
        // Without using the testharness. Could be useful if you wanted to run
        // The service over multiple machines.
        public static void Main(string[] args)
        {
            var worker = new TaskWorker(args[0], args[1]);
        }
    }

    public class TaskWorker
    {
        private string _receiverAddress;
        private string _senderAddress;

        public CancellationToken Token;

        public TaskWorker(string receiverAddress, string senderAddress)
        {
            _receiverAddress = receiverAddress;
            _senderAddress = senderAddress;

            Token = new CancellationToken();
        }

        public void DoWork()
        {
            using (var context = NetMQContext.Create())
            {
                using (NetMQSocket receiver = context.CreatePullSocket(),
                                   sender = context.CreatePushSocket())
                {
                    receiver.Connect(_receiverAddress);
                    sender.Connect(_senderAddress);

                    // Allows us to remotely kill the process
                    while (!Token.IsCancellationRequested)
                    {
                        string taskString = receiver.ReceiveString();

                        var tasks = taskString.Split(',');

                        var job = tasks[0]; // First index is Job id

                        var result = new JobResult(job);

                        // Our 'work'
                        foreach (var word in tasks.Skip(1).Distinct())
                        {
                            result.Data[word] = tasks.Count(p => p == word);
                        }

                        // Send 'result' to the sink
                        sender.Send(JsonConvert.SerializeObject(result));
                    }
                }
            }
        }
    }
}
