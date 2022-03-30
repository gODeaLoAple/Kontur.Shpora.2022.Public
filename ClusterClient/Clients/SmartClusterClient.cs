using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class SmartClusterClient : ClusterClientBase
{
	public SmartClusterClient(string[] replicaAddresses)
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
		var tasks = new List<Task<string>>();
		var sw = Stopwatch.StartNew();
		foreach (var request in requests)
		{
			var stepTimeout = timeout / n;
			var task = ProcessRequestAsync(request);
			tasks.Add(task);
			var aggregateTask = Task.WhenAny(tasks);
			
			sw.Restart();
			await Task.WhenAny(aggregateTask, Task.Delay(stepTimeout));
			sw.Stop();
			if (aggregateTask.IsCompleted)
			{
				var completedTask = aggregateTask.Result;
				if (completedTask.IsFaulted)
				{
					tasks.Remove(completedTask);
				}
				else
				{
					return completedTask.Result;
				}
			}

			timeout -= sw.Elapsed;
			n = Math.Max(n - 1, 1);
		}
	
		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}