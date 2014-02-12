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
	public class PutGet : SyncExample
	{
		public PutGet(Console console) : base(console)
		{
		}

		/// <summary>
		/// Write and read a bin value.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (args.singleBin)
			{
				RunSingleBinTest(client, args);
			}
			else
			{
				RunMultiBinTest(client, args);
			}
			RunGetHeaderTest(client, args);
		}

		/// <summary>
		/// Execute put and get on a server configured as multi-bin.  This is the server default.
		/// </summary>
		private void RunMultiBinTest(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "putgetkey");
			Bin bin1 = new Bin("bin1", "value1");
			Bin bin2 = new Bin("bin2", "value2");

			console.Info("Put: namespace={0} set={1} key={2} bin1={3} value1={4} bin2={5} value2={6}", 
				key.ns, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

			client.Put(args.writePolicy, key, bin1, bin2);

			console.Info("Get: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

			Record record = client.Get(args.policy, key);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			ValidateBin(key, bin1, record);
			ValidateBin(key, bin2, record);
		}

		/// <summary>
		/// Execute put and get on a server configured as single-bin.
		/// </summary>
		private void RunSingleBinTest(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "putgetkey");
			Bin bin = new Bin("", "value");

			console.Info("Single Bin Put: namespace={0} set={1} key={2} value={3}", 
				key.ns, key.setName, key.userKey, bin.value);

			client.Put(args.writePolicy, key, bin);

			console.Info("Single Bin Get: namespace={0} set={1} key={2}", key.ns, 
				key.setName, key.userKey);

			Record record = client.Get(args.policy, key);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			ValidateBin(key, bin, record);
		}

		private void ValidateBin(Key key, Bin bin, Record record)
		{
			object received = record.GetValue(bin.name);
			string expected = bin.value.ToString();

			if (received != null && received.Equals(expected))
			{
				console.Info("Bin matched: namespace={0} set={1} key={2} bin={3} value={4} generation={5} expiration={6}", 
					key.ns, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
			}
			else
			{
				console.Error("Put/Get mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}

		/// <summary>
		/// Read record header data.
		/// </summary>
		private void RunGetHeaderTest(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "putgetkey");

			console.Info("Get record header: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);
			Record record = client.GetHeader(args.policy, key);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			// Generation should be greater than zero.  Make sure it's populated.
			if (record.generation == 0)
			{
				throw new Exception(string.Format("Invalid record header: generation={0:D} expiration={1:D}", 
					record.generation, record.expiration));
			}
			console.Info("Received: generation={0} expiration={1}", record.generation, record.expiration);
		}
	}
}
