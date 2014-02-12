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
namespace Aerospike.Client
{
	public sealed class AsyncBatchExistsSequence : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequence
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			ExistsSequenceListener listener
		) : base(parent, cluster, node, false)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.listener = listener;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchExists(batchNamespace);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
			listener.OnExists(key, resultCode == 0);
		}
	}
}
