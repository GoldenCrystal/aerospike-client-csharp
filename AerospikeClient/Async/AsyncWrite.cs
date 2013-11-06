/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	public sealed class AsyncWrite : AsyncSingleCommand
	{
		private readonly WritePolicy policy;
		private readonly WriteListener listener;
		private readonly Bin[] bins;
		private readonly Operation.Type operation;

		public AsyncWrite(AsyncCluster cluster, WritePolicy policy, WriteListener listener, Key key, Bin[] bins, Operation.Type operation) 
			: base(cluster, key)
		{
			this.policy = (policy == null)? new WritePolicy() : policy;
			this.listener = listener;
			this.bins = bins;
			this.operation = operation;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetWrite(policy, operation, key, bins);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[5];

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}