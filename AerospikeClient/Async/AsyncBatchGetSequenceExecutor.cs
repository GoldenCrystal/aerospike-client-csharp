/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncBatchGetSequenceExecutor : AsyncBatchExecutor
	{
		private readonly RecordSequenceListener listener;

		public AsyncBatchGetSequenceExecutor(AsyncCluster cluster, Policy policy, RecordSequenceListener listener, Key[] keys, HashSet<string> binNames, int readAttr) 
			: base(cluster, keys)
		{
			this.listener = listener;

			if (policy == null)
			{
				policy = new Policy();
			}

			// Dispatch asynchronous commands to nodes.
			foreach (BatchNode batchNode in batchNodes)
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					AsyncBatchGetSequence async = new AsyncBatchGetSequence(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, binNames, listener, readAttr);
					async.Execute();
				}
			}
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}
}
