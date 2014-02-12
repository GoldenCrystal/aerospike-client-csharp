/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class BatchExecutor
	{
		private readonly Thread thread;
		private readonly Policy policy;
		private readonly BatchNode batchNode;
		private readonly HashSet<string> binNames;
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly Record[] records;
		private readonly bool[] existsArray;
		private readonly int readAttr;
		private Exception exception;

		public BatchExecutor(Policy policy, BatchNode batchNode, Dictionary<Key, BatchItem> keyMap, HashSet<string> binNames, Record[] records, bool[] existsArray, int readAttr)
		{
			this.policy = policy;
			this.batchNode = batchNode;
			this.keyMap = keyMap;
			this.binNames = binNames;
			this.records = records;
			this.existsArray = existsArray;
			this.readAttr = readAttr;
			this.thread = new Thread(new ThreadStart(this.Run));
		}

		public void Start()
		{
			thread.Start();
		}

		public void Join()
		{
			thread.Join();
		}

		public void Run()
		{
			try
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					if (records != null)
					{
						BatchCommandGet command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keyMap, binNames, records, readAttr);
						command.Execute();
					}
					else
					{
						BatchCommandExists command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keyMap, existsArray);
						command.Execute();
					}
				}
			}
			catch (Exception e)
			{
				exception = e;
			}
		}

		public Exception Exception
		{
			get
			{
				return exception;
			}
		}

		public static void ExecuteBatch(Cluster cluster, Policy policy, Key[] keys, bool[] existsArray, Record[] records, HashSet<string> binNames, int readAttr)
		{
			Dictionary<Key, BatchItem> keyMap = BatchItem.GenerateMap(keys);
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, keys);

			// Dispatch the work to each node on a different thread.
			List<BatchExecutor> threads = new List<BatchExecutor>(batchNodes.Count);

			foreach (BatchNode batchNode in batchNodes)
			{
				BatchExecutor thread = new BatchExecutor(policy, batchNode, keyMap, binNames, records, existsArray, readAttr);
				threads.Add(thread);
				thread.Start();
			}

			// Wait for all the threads to finish their work and return results.
			foreach (BatchExecutor thread in threads)
			{
				try
				{
					thread.Join();
				}
				catch (Exception)
				{
				}
			}

			// Throw an exception if an error occurred.
			foreach (BatchExecutor thread in threads)
			{
				Exception e = thread.Exception;

				if (e != null)
				{
					if (e is AerospikeException)
					{
						throw (AerospikeException)e;
					}
					else
					{
						throw new AerospikeException(e);
					}
				}
			}
		}
	}
}
