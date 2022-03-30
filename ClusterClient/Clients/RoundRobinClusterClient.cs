using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class RoundRobinClusterClient : ClusterClientBase
{

	public RoundRobinClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{
		if (replicaAddresses.Length == 0)
			throw new ArgumentException("Expected addresses count to be positive, but actual zero.");
	}

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var requests = ReplicaAddresses
			.Select(uri => CreateRequestWithQuery(uri, query))
			.ToArray();

		var n = requests.Length;
		var sw = Stopwatch.StartNew();
		foreach (var request in requests)
		{
			var stepTimeout = timeout / n;
			var task = ProcessRequestAsync(request);
			Log.InfoFormat($"Processing {request.RequestUri}");
			sw.Restart();
			await Task.WhenAny(task, Task.Delay(stepTimeout));
			sw.Stop();
			
			if (task.IsCompleted && !task.IsFaulted)
			{
				return task.Result;
			}

			timeout -= sw.Elapsed;
			n = Math.Max(n - 1, 1);
		}

		throw new TimeoutException();
	}
	
	protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}