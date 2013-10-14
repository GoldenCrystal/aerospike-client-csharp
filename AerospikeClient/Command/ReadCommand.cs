/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ReadCommand : SingleCommand
	{
		private Record record;
		private int resultCode;

		public ReadCommand(Cluster cluster, Key key) : base(cluster, key)
		{
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(receiveBuffer, MSG_TOTAL_HEADER_SIZE);

			// A number of these are commented out because we just don't care enough to read
			// that section of the header. If we do care, uncomment and check!        
			long sz = ByteUtil.BytesToLong(receiveBuffer, 0);
			byte headerLength = receiveBuffer[8];
	//		byte info1 = header[9];
	//		byte info2 = header[10];
	//      byte info3 = header[11];
	//      byte unused = header[12];
			resultCode = receiveBuffer[13];
			int generation = ByteUtil.BytesToInt(receiveBuffer, 14);
			int expiration = ByteUtil.BytesToInt(receiveBuffer, 18);
	//		int transactionTtl = get_ntohl(header, 22);
			int fieldCount = ByteUtil.BytesToShort(receiveBuffer, 26); // almost certainly 0
			int opCount = ByteUtil.BytesToShort(receiveBuffer, 28);
			int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;
			/*
			byte version = (byte) (((int)(sz >> 56)) & 0xff);
			if (version != MSG_VERSION) {
				if (Log.debugEnabled()) {
					Log.debug("read header: incorrect version.");
				}
			}
			
			byte type = (byte) (((int)(sz >> 48)) & 0xff);
			if (type != MSG_TYPE) {
				if (Log.debugEnabled()) {
					Log.debug("read header: incorrect message type, aborting receive");
				}
			}
			
			if (headerLength != MSG_REMAINING_HEADER_SIZE) {
				if (Log.debugEnabled()) {
					Log.debug("read header: unexpected header size, aborting");
				}
			}*/

			// Read remaining message bytes.
			if (receiveSize > 0)
			{
				ResizeReceiveBuffer(receiveSize);
				conn.ReadFully(receiveBuffer, receiveSize);
			}

			if (resultCode != 0)
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					return;
				}

				if (resultCode == ResultCode.UDF_BAD_RESPONSE)
				{
					record = ParseRecord(opCount, fieldCount, generation, expiration);
					HandleUdfError();
				}
				throw new AerospikeException(resultCode);
			}

			if (opCount == 0)
			{
				// Bin data was not returned.
				record = new Record(null, generation, expiration);
				return;
			}
			record = ParseRecord(opCount, fieldCount, generation, expiration);
		}

		private void HandleUdfError()
		{
			object obj;

			if (record.bins.TryGetValue("FAILURE", out obj))
			{
				string ret = (string)obj;
				string[] list;
				string message;
				int code;

				try
				{
					list = ret.Split(':');
					code = Convert.ToInt32(list[2].Trim());
					message = list[0] + ':' + list[1] + ' ' + list[3];
				}
				catch (Exception)
				{
					// Use generic exception if parse error occurs.
					throw new AerospikeException(resultCode, ret);
				}

				throw new AerospikeException(code, message);
			}
		}

		private Record ParseRecord(int opCount, int fieldCount, int generation, int expiration)
		{
			Dictionary<string, object> bins = null;
			int receiveOffset = 0;

			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			if (fieldCount != 0)
			{
				// Just skip over all the fields
				for (int i = 0; i < fieldCount; i++)
				{
					int fieldSize = ByteUtil.BytesToInt(receiveBuffer, receiveOffset);
					receiveOffset += 4 + fieldSize;
				}
			}

			for (int i = 0 ; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(receiveBuffer, receiveOffset);
				byte particleType = receiveBuffer[receiveOffset + 5];
				byte nameSize = receiveBuffer[receiveOffset + 7];
				string name = ByteUtil.Utf8ToString(receiveBuffer, receiveOffset + 8, nameSize);
				receiveOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
				object value = ByteUtil.BytesToParticle(particleType, receiveBuffer, receiveOffset, particleBytesSize);
				receiveOffset += particleBytesSize;

				if (bins == null)
				{
					bins = new Dictionary<string, object>();
				}
				bins[name] = value;
			}
			return new Record(bins, generation, expiration);
		}

		public Record Record
		{
			get
			{
				return record;
			}
		}

		public int GetResultCode()
		{
			return resultCode;
		}
	}
}