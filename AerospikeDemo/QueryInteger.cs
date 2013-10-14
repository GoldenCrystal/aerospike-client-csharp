using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryInteger : SyncExample
	{
		public QueryInteger(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index on an integer bin and query on it.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}

			string indexName = "queryindexint";
			string keyPrefix = "querykeyint";
			string binName = args.GetBinName("querybinint");
			int size = 50;

			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, size);
			RunQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);

			// The server index command distribution to other nodes is done asynchronously.  
			// Therefore, the server may return before the index is available on all nodes.  
			// Hard code sleep for now.
			// TODO: Fix server so control is only returned when index is available on all nodes.
			Util.Sleep(1000);
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
		{
			console.Info("Write " + size + " records.");

			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);
				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			Value begin = Value.Get(14);
			Value end = Value.Get(18);

			console.Info("Query for: ns={0} set={1} index={2} bin={3} >= {4} <= {5}", 
				args.ns, args.set, indexName, binName, begin, end);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Range(binName, begin, end));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					long result = (long)record.GetValue(binName);

					console.Info("Record found: namespace={0} set={1} digest={2} bin={3} value={4}",
						key.ns, key.setName, ByteUtil.BytesToHexString(key.digest), binName, result);

					count++;
				}

				if (count != 5)
				{
					console.Error("Query count mismatch. Expected 5. Received " + count);
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}