/* 
 * Copyright 2012-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Net.Sockets;

namespace Aerospike.Client
{
	internal sealed class AsyncCommandQueue
	{
		private readonly ConcurrentQueue<SocketAsyncEventArgs> _argsQueue = new ConcurrentQueue<SocketAsyncEventArgs>();
		private readonly ConcurrentQueue<AsyncCommand> _commandQueue;
		private readonly WaitCallback _schedulingJobCallback;
		private volatile int _jobScheduled;

		public AsyncCommandQueue(bool blocking)
		{
			// Only allocate a concurrent queue of AsyncCommand objects if the blocking mode was requested.
			// In non blocking mode, we won't have to queue commands for later execution so, it's useless.
			if (blocking)
			{
				_commandQueue = new ConcurrentQueue<AsyncCommand>();
				_schedulingJobCallback = new WaitCallback(ExclusiveScheduleCommands);
			}
		}

		// Releases a SocketEventArgs object to the pool.
		public void ReleaseArgs(SocketAsyncEventArgs e)
		{
			AsyncCommand command;

			if (_commandQueue != null && _commandQueue.TryDequeue(out command))
			{
				command.ExecuteAsync(e);
			}
			else
			{
				_argsQueue.Enqueue(e);
				if (_commandQueue != null)
				{
					TriggerCommandScheduling();
				}
			}
		}

		// Schedules a command for later execution.
		public bool ScheduleCommand(AsyncCommand command)
		{
			SocketAsyncEventArgs e;

			// Try to dequeue one SocketAsyncEventArgs object from the queue and execute it.
			if (_argsQueue.TryDequeue(out e))
			{
				// If there are no awaiting command, the current command can be executed immediately.
				if (_commandQueue == null || _commandQueue.IsEmpty) // NB: We could make the choice to always execute the command synchronously in this case. Might be better for eprformance.
				{
					command.ExecuteInline(e);
					return true;
				}
				else
				{
					_argsQueue.Enqueue(e);
				}
			}
			else if (_commandQueue != null)
			{
				// In blocking mode, the command can be queued for later execution.
				_commandQueue.Enqueue(command);
			}
			else
			{
				// In non-blocking mode, we can't queue commands. Scheduling will fail if no SocketAsyncEventArgs object is available.
				return false;
			}

			TriggerCommandScheduling();
			return true;
		}

		// Schedule exactly once the job that will execute queued commands.
		private void TriggerCommandScheduling()
		{
			if (Interlocked.CompareExchange(ref _jobScheduled, 1, 0) == 0)
			{
#if NETCORE && !NETSTANDARD2_0
				ThreadPool.QueueUserWorkItem(_schedulingJobCallback, null);
#else
				ThreadPool.UnsafeQueueUserWorkItem(_schedulingJobCallback, null);
#endif
			}
		}

		// Schedule as many commands as possible.
		private void ExclusiveScheduleCommands(object state)
		{
			do
			{
				bool lockTaken = false;
				try
				{
					// Lock on _commandQueue for exclusive execution of the job.
					Monitor.TryEnter(_commandQueue, ref lockTaken); // If we can't enter the lock, it means another instance of the job is already doing the work.
					if (!lockTaken) return;

					_jobScheduled = 1; // Volatile Write. At this point, the job cannot be rescheduled.

					// Try scheduling as many commands as possible.
					SocketAsyncEventArgs e;
					while (!_commandQueue.IsEmpty && _argsQueue.TryDequeue(out e))
					{
						AsyncCommand dequeuedCommand;
						if (_commandQueue.TryDequeue(out dequeuedCommand))
						{
							dequeuedCommand.ExecuteAsync(e);
						}
						else
						{
							_argsQueue.Enqueue(e);
						}
					}
				}
				finally
				{
					if (lockTaken)
					{
						_jobScheduled = 0; // Volatile Write. At this point, the job can be rescheduled.
						Monitor.Exit(_commandQueue);
					}
				}
			}
			while (!(_commandQueue.IsEmpty || _argsQueue.IsEmpty)); // Re-execute the job as long as both queues are non-empty
		}
	}
}
