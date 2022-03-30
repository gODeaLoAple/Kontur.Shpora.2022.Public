using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class RoundRobinClusterClient : ClusterClientBase
{
	private readonly Dictionary<string, int> priority;

	public RoundRobinClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{
		if (replicaAddresses.Length == 0)
			throw new ArgumentException("Expected addresses count to be positive, but actual zero.");
		priority = replicaAddresses.ToDictionary(x => x, x => 0);
	}

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var addresses = ReplicaAddresses
			.OrderBy(x => priority[x])
			.ToArray();

		var n = addresses.Length;
		var sw = Stopwatch.StartNew();
		foreach (var address in addresses)
		{
			var stepTimeout = timeout / n;
			var request = CreateRequestWithQuery(address, query);
			var task = ProcessRequestAsync(request);
			Log.InfoFormat($"Processing {request.RequestUri}");
			sw.Restart();
			await Task.WhenAny(task, Task.Delay(stepTimeout));
			sw.Stop();
			priority[address] += (int)sw.ElapsedMilliseconds;
			
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