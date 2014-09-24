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
namespace Aerospike.Client
{
	/// <summary>
	/// Configuration variables for multi-record get and exist requests.
	/// </summary>
	public sealed class BatchPolicy : Policy
	{
		/// <summary>
		/// Maximum number of concurrent batch request threads to server nodes at any point in time.
		/// If there are 16 node/namespace combinations requested and maxConcurrentThreads is 8, 
		/// then batch requests will be made for 8 node/namespace combinations in parallel threads.
		/// When a request completes, a new request will be issued until all 16 threads are complete.
		/// <para>
		/// Values:
		/// <list type="bullet">
		/// <item>
		/// 1: Issue batch requests sequentially.  This mode has a performance advantage for small
		/// to medium sized batch sizes because requests can be issued in the main transaction thread.
		/// This is the default.
		/// </item>
		/// <item>
		/// 0: Issue all batch requests in parallel threads.  This mode has a performance
		/// advantage for extremely large batch sizes because each node can process the request
		/// immediately.  The downside is extra threads will need to be created (or taken from
		/// a thread pool).
		/// </item>
		/// <item>
		/// > 0: Issue up to maxConcurrentThreads batch requests in parallel threads.  When a request
		/// completes, a new request will be issued until all threads are complete.  This mode
		/// prevents too many parallel threads being created for large cluster implementations.
		/// The downside is extra threads will still need to be created (or taken from a thread pool).
		/// </item>
		/// </list>
		/// </para>
		/// </summary>		
		public int maxConcurrentThreads = 1;

		/// <summary>
		/// Copy batch policy from another batch policy.
		/// </summary>
		public BatchPolicy(BatchPolicy other)
			: base(other)
		{
			this.maxConcurrentThreads = other.maxConcurrentThreads;
		}

		/// <summary>
		/// Copy batch policy from another policy.
		/// </summary>
		public BatchPolicy(Policy other)
			: base(other)
		{
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public BatchPolicy()
		{
		}
	}
}
