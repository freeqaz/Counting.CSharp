using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Counting.Shared
{
    /// <summary>
    /// Holds the results of a job along with it's identifier.
    /// </summary>
    [Serializable]
    public class JobResult
    {
        public string JobId { get; private set; }

        public Dictionary<string, int> Data { get; private set; }

        public JobResult(string jobId)
        {
            JobId = jobId;
            Data = new Dictionary<string, int>();
        }
    }
}
