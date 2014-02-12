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
using System.Threading;
using System.Collections.Concurrent;
using LuaInterface;

namespace Aerospike.Client
{
	public sealed class QueryAggregateCommand : QueryCommand
	{
		private readonly BlockingCollection<object> inputQueue;

		public QueryAggregateCommand(Node node, Policy policy, Statement statement, BlockingCollection<object> inputQueue)
			: base(node, policy, statement)
		{
			this.inputQueue = inputQueue;
		}

		protected internal override bool ParseRecordResults(int receiveSize)
		{
			// Read/parse remaining message bytes one record at a time.
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				ReadBytes(MSG_REMAINING_HEADER_SIZE);
				int resultCode = dataBuffer[5];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						return false;
					}
					throw new AerospikeException(resultCode);
				}

				byte info3 = dataBuffer[3];

				// If this is the end marker of the response, do not proceed further
				if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST)
				{
					return false;
				}

				int fieldCount = ByteUtil.BytesToShort(dataBuffer, 18);
				int opCount = ByteUtil.BytesToShort(dataBuffer, 20);

				ParseKey(fieldCount);

				if (opCount != 1)
				{
					throw new AerospikeException("Query aggregate expected exactly one bin.  Received " + opCount);
				}

				// Parse aggregateValue.
				ReadBytes(8);
				int opSize = ByteUtil.BytesToInt(dataBuffer, 0);
				byte particleType = dataBuffer[5];
				byte nameSize = dataBuffer[7];

				ReadBytes(nameSize);
				string name = ByteUtil.Utf8ToString(dataBuffer, 0, nameSize);

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				ReadBytes(particleBytesSize);
				object aggregateValue = LuaInstance.BytesToLua(particleType, dataBuffer, 0, particleBytesSize);

				if (!name.Equals("SUCCESS"))
				{
					throw new AerospikeException("Query aggregate expected bin name SUCCESS.  Received " + name);
				}

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

				if (aggregateValue != null)
				{
					try
					{
						inputQueue.Add(aggregateValue);
					}
					catch (ThreadInterruptedException)
					{
					}
				}
			}
			return true;
		}
	}
}
