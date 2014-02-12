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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ScanParallel : SyncExample
	{
		private int recordCount = 0;

		public ScanParallel(Console console) : base(console)
		{
		}

		/// <summary>
		/// Scan all nodes in parallel and read all records in a set.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			console.Info("Scan parallel: namespace=" + args.ns + " set=" + args.set);
			recordCount = 0;
			DateTime begin = DateTime.Now;
			ScanPolicy policy = new ScanPolicy();
			client.ScanAll(policy, args.ns, args.set, ScanCallback);

			DateTime end = DateTime.Now;
			double seconds = end.Subtract(begin).TotalSeconds;
			console.Info("Total records returned: " + recordCount);
			console.Info("Elapsed time: " + seconds + " seconds");
			double performance = Math.Round((double)recordCount / seconds);
			console.Info("Records/second: " + performance);
		}

		public void ScanCallback(Key key, Record record)
		{
			recordCount++;

			if ((recordCount % 10000) == 0)
			{
				console.Info("Records " + recordCount);
			}
		}
	}
}
