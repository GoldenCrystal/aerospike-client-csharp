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
using System.Threading;
using Aerospike.Client;
using System.Diagnostics;

namespace Aerospike.Demo
{
	class BenchmarkThreadAsync : BenchmarkThread
	{
		private AsyncClient client;
        private SemaphoreSlim semaphore;
        private int concurrencyLimit;

        public BenchmarkThreadAsync
        (
            Console console,
            BenchmarkArguments args,
            BenchmarkShared shared,
            Example example,
            CountdownEvent initCountdownEvent,
            AsyncClient client
        ) : base(console, args, shared, example, initCountdownEvent)
		{
			this.client = client;
            this.concurrencyLimit = Math.Max(1, args.commandMax / args.threadMax); // Don't start too many async operations at the same time. (This prevents the read/write benchmark of queing a huge number of asynchronous operations)
            this.semaphore = new SemaphoreSlim(concurrencyLimit, concurrencyLimit);
        }

        protected override void WaitForCompletion()
        {
            base.WaitForCompletion();

            // Starve the semaphore… Thus waiting for all asynchronous commands to complete.
            for (int i = 0; i < concurrencyLimit; i++)
            {
                semaphore.Wait();
            }
        }

        protected override void SignalCompletedOperation()
        {
            base.SignalCompletedOperation();

            // Notify that one operation that was started has completed.
            semaphore.Release();
        }

        protected override void WriteRecord(WritePolicy policy, Key key, Bin bin)
		{
			// If timeout occurred, yield thread to back off throttle.
			// Fail counters are reset every second.
			if (shared.writeTimeoutCount > 0)
			{
				Thread.Yield();
			}

            // Wait for one free slot before starting the async operation.
            semaphore.Wait();

            if (shared.writeLatency != null)
            {
                client.Put(policy, new LatencyWriteHandler(this, key, bin), key, bin);
            }
            else
            {
                client.Put(policy, new WriteHandler(this, key, bin), key, bin);
            }
		}

        protected override void ReadRecord(Policy policy, Key key, string binName)
		{
			// If timeout occurred, yield thread to back off throttle.
			// Fail counters are reset every second.
			if (shared.readTimeoutCount > 0)
			{
				Thread.Yield();
			}

            // Wait for one free slot before starting the async operation.
            semaphore.Wait();

            if (shared.readLatency != null)
            {
                client.Get(policy, new LatencyReadHandler(this, key), key, binName);
            }
            else
            {
                client.Get(policy, new ReadHandler(this, key), key, binName);
            }
		}

		private class WriteHandler : WriteListener
		{
            BenchmarkThreadAsync parent;
            Key key;
            Bin bin;

            public WriteHandler(BenchmarkThreadAsync parent, Key key, Bin bin)
			{
				this.parent = parent;
                this.key = key;
                this.bin = bin;
			}

			public void OnSuccess(Key k)
			{
				parent.OnWriteSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnWriteFailure(key, bin, e);
            }
        }

        private class LatencyWriteHandler : WriteListener
        {
            BenchmarkThreadAsync parent;
            Key key;
            Bin bin;
            Stopwatch watch;

            public LatencyWriteHandler(BenchmarkThreadAsync parent, Key key, Bin bin)
            {
                this.parent = parent;
                this.key = key;
                this.bin = bin;
                this.watch = Stopwatch.StartNew();
            }

            public void OnSuccess(Key k)
            {
                double elapsed = watch.Elapsed.TotalMilliseconds;
                parent.OnWriteSuccess(elapsed);
            }

            public void OnFailure(AerospikeException e)
            {
                parent.OnWriteFailure(key, bin, e);
            }
        }
        
        private class ReadHandler : RecordListener
		{
            BenchmarkThreadAsync parent;
			Key key;

            public ReadHandler(BenchmarkThreadAsync parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key k, Record record)
			{
				parent.OnReadSuccess();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.OnReadFailure(key, e);
			}
		}
    
        private class LatencyReadHandler : RecordListener
        {
            BenchmarkThreadAsync parent;
            Key key;
            Stopwatch watch;

            public LatencyReadHandler(BenchmarkThreadAsync parent, Key key)
            {
                this.parent = parent;
                this.key = key;
                this.watch = Stopwatch.StartNew();
            }

            public void OnSuccess(Key k, Record record)
            {
                double elapsed = watch.Elapsed.TotalMilliseconds;
                parent.OnReadSuccess(elapsed);
            }

            public void OnFailure(AerospikeException e)
            {
                parent.OnReadFailure(key, e);
            }
        }
    }
}
