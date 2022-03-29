using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class ParallelClusterClient : ClusterClientBase
{
	public ParallelClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{
		
	}

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var requestTasks = ReplicaAddresses
			.Select(uri => CreateRequest(uri + "?query=" + query))
			.Select(request =>
			{
				Log.InfoFormat($"Processing {request.RequestUri}");
				return ProcessRequestAsync(request);
			})
			.ToList();

		var aggregateTask = Task.Run(async () => await GetFirstNotFaultedTask(requestTasks));
		await Task.WhenAny(aggregateTask, Task.Delay(timeout));
		if (!aggregateTask.IsCompleted)
			throw new TimeoutException();
		return aggregateTask.Result;
	}

	private static async Task<T> GetFirstNotFaultedTask<T>(ICollection<Task<T>> tasks)
	{
		while (tasks.Count > 0)
		{
			var completedTask = await Task.WhenAny(tasks);

			if (completedTask.IsFaulted)
			{
				tasks.Remove(completedTask);
			}
			else
			{
				return completedTask.Result;
			}
		}

		throw new InvalidOperationException("All tasks were faulted.");
	}

	protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
}