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
using Aerospike.Client;

namespace Aerospike.Demo
{
	abstract class BenchmarkThread
	{
        private static int seed = Environment.TickCount;

		protected readonly Console console;
        protected readonly BenchmarkArguments args;
        protected readonly BenchmarkShared shared;
        private readonly Example example;
        private readonly Random random;
        private Thread thread;

        public BenchmarkThread(Console console, BenchmarkArguments args, BenchmarkShared shared, Example example)
		{
			this.console = console;
            this.args = args;
            this.shared = shared;
            this.example = example;
            random = new Random(Interlocked.Increment(ref seed));
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
            }
            catch (Exception ex)
            {
                console.Error(ex.Message);
            }
        }

        public void Join()
        {
            thread.Join();
            thread = null;
        }

        private void InitRecords()
        {
            while (example.valid)
            {
                int key = Interlocked.Increment(ref shared.currentKey);

                if (key >= args.recordsInit)
                {
                    if (key == args.recordsInit)
                    {
                        console.Info("write(tps={0} timeouts={1} errors={2} total={3}))",
                            shared.writeCount, shared.writeTimeoutCount, shared.writeErrorCount, args.recordsInit
                        );
                    }
                    break;
                }
                Write(key);
            }
        }
        
		private void RunWorker()
		{
            while (example.valid)
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

		protected void OnWriteSuccess()
		{
			Interlocked.Increment(ref shared.writeCount);
		}

        protected void OnWriteSuccess(double elapsed)
        {
            Interlocked.Increment(ref shared.writeCount);
            shared.writeLatency.Add(elapsed);
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
	    }

		protected void OnWriteFailure(Key key, Bin bin, Exception e)
		{
			Interlocked.Increment(ref shared.writeErrorCount);
			
            if (args.debug)
			{
				console.Error("Write error: ns={0} set={1} key={2} bin={3} exception={4}",
                    key.ns, key.setName, key.userKey, bin.name, e.Message);
			}
	    }

		protected void OnReadSuccess()
		{
            Interlocked.Increment(ref shared.readCount);
		}

        protected void OnReadSuccess(double elapsed)
        {
            Interlocked.Increment(ref shared.readCount);
            shared.readLatency.Add(elapsed);
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
		}
		
		protected void OnReadFailure(Key key, Exception e)
		{
			Interlocked.Increment(ref shared.readErrorCount);

			if (args.debug)
			{
				console.Error("Read error: ns={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);
			}
		}

		protected abstract void WriteRecord(WritePolicy policy, Key key, Bin bin);
		protected abstract void ReadRecord(Policy policy, Key key, string binName);
	}
}
