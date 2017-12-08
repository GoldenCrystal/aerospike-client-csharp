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

namespace Aerospike.Demo
{
	abstract class BenchmarkThread
	{
		protected readonly Console console;
        protected readonly BenchmarkArguments args;
        protected readonly BenchmarkShared shared;
        protected readonly Example example;
        private readonly RandomShift random;
        private Thread thread;
        private readonly CountdownEvent initCountdownEvent;
        
        public BenchmarkThread(Console console, BenchmarkArguments args, BenchmarkShared shared, Example example, CountdownEvent initCountdownEvent)
		{
			this.console = console;
            this.args = args;
            this.shared = shared;
            this.example = example;
			random = new RandomShift();
            this.initCountdownEvent = initCountdownEvent;
        }

        public void Start()
        {
            thread = new Thread(new ThreadStart(this.Run));
            thread.Start();
        }

        public void Run()
        {
            try
            {
                if (args.recordsInit > 0)
                {
                    InitRecords();
                }
                else
                {
                    RunWorker();
                }

                WaitForCompletion();
            }
            catch (Exception ex)
            {
                console.Error(ex.Message);
            }
        }

        protected virtual void WaitForCompletion()
        {
            // Wait for either the cancellation of the benchmark, or the completion of the initialization.
            WaitHandle.WaitAny(new[] { example.WaitHandle, initCountdownEvent.WaitHandle });
        }

        public void Join()
        {
            thread.Join();
            thread = null;
        }

        private void InitRecords()
        {
            while (example.Valid)
            {
			    int key = Interlocked.Increment(ref shared.currentKey);

                if (key >= args.recordsInit)
                {
                    if (key == args.recordsInit)
                    {
                        initCountdownEvent.Wait();
                        console.Info("write(tps={0} timeouts={1} errors={2} total={3}))",
                            shared.writeCount, shared.writeTimeoutCount, shared.writeErrorCount, shared.completedOperationCount
                        );
                    }
                    break;
                }
                Write(key);
			}
        }
        
		private void RunWorker()
		{
            while (example.Valid)
            {
                // Choose key at random.
                int key = random.Next(0, args.records);

                // Roll a percentage die.
                int die = random.Next(0, 100);

                if (die < args.readPct)
                {
                    Read(key);
                }
                else
                {
                    Write(key);
                }
            }
		}

		private void Write(int userKey)
		{
            Key key = new Key(args.ns, args.set, userKey);
            Bin bin = new Bin(args.binName, args.GetValue(random));

			try
			{
				WriteRecord(args.writePolicy, key, bin);
			}
			catch (AerospikeException ae)
			{
				OnWriteFailure(key, bin, ae);
			}
			catch (Exception e)
			{
				OnWriteFailure(key, bin, e);
			}
		}

        private void Read(int userKey)
		{
            Key key = new Key(args.ns, args.set, userKey);
            
            try
			{
                ReadRecord(args.writePolicy, key, args.binName);
			}
			catch (AerospikeException ae)
			{
				OnReadFailure(key, ae);
			}
			catch (Exception e)
			{
				OnReadFailure(key, e);
			}
		}

        protected virtual void SignalCompletedOperation()
        {
            Interlocked.Increment(ref shared.completedOperationCount);
        }

        private void SignalCompletedWrite()
        {
            if (initCountdownEvent.InitialCount > 0)
            {
                initCountdownEvent.Signal();
            }
            SignalCompletedOperation();
        }

        private void SignalCompletedRead()
        {
            SignalCompletedOperation();
        }

		protected void OnWriteSuccess()
		{
			Interlocked.Increment(ref shared.writeCount);
            SignalCompletedWrite();
        }

        protected void OnWriteSuccess(double elapsed)
        {
            Interlocked.Increment(ref shared.writeCount);
            shared.writeLatency.Add(elapsed);
            SignalCompletedWrite();
        }

        protected void OnWriteFailure(Key key, Bin bin, AerospikeException ae)
		{
			if (ae.Result == ResultCode.TIMEOUT)
			{
				Interlocked.Increment(ref shared.writeTimeoutCount);
			}
			else
			{
				Interlocked.Increment(ref shared.writeErrorCount);

				if (args.debug)
				{
					console.Error("Write error: ns={0} set={1} key={2} bin={3} exception={4}",
						key.ns, key.setName, key.userKey, bin.name, ae.Message);
				}
			}
            SignalCompletedWrite();
        }

        protected void OnWriteFailure(Key key, Bin bin, Exception e)
		{
			Interlocked.Increment(ref shared.writeErrorCount);
			
            if (args.debug)
			{
				console.Error("Write error: ns={0} set={1} key={2} bin={3} exception={4}",
                    key.ns, key.setName, key.userKey, bin.name, e.Message);
			}
            SignalCompletedWrite();
        }

        protected void OnReadSuccess()
		{
            Interlocked.Increment(ref shared.readCount);
            SignalCompletedRead();
        }

        protected void OnReadSuccess(double elapsed)
        {
            Interlocked.Increment(ref shared.readCount);
            shared.readLatency.Add(elapsed);
            SignalCompletedRead();
        }

        protected void OnReadFailure(Key key, AerospikeException ae)
		{
			if (ae.Result == ResultCode.TIMEOUT)
			{
				Interlocked.Increment(ref shared.readTimeoutCount);
			}
			else
			{
				Interlocked.Increment(ref shared.readErrorCount);

				if (args.debug)
				{
					console.Error("Read error: ns={0} set={1} key={2} exception={3}",
						key.ns, key.setName, key.userKey, ae.Message);
				}
			}
            SignalCompletedRead();
        }

        protected void OnReadFailure(Key key, Exception e)
		{
			Interlocked.Increment(ref shared.readErrorCount);

			if (args.debug)
			{
				console.Error("Read error: ns={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
			}
            SignalCompletedRead();
        }

        protected abstract void WriteRecord(WritePolicy policy, Key key, Bin bin);
		protected abstract void ReadRecord(Policy policy, Key key, string binName);
	}
}
