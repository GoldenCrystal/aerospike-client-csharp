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
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Aerospike exceptions that can be thrown from the client.
	/// </summary>
	public class AerospikeException : Exception
	{
		private const long serialVersionUID = 1L;

		private int resultCode;

        public AerospikeException(int resultCode, string message) 
            : base(message)
		{
            this.resultCode = resultCode;
		}

        public AerospikeException(int resultCode, Exception e) 
            : base(e.Message, e)
		{
            this.resultCode = resultCode;
		}

        public AerospikeException(int resultCode) 
            : base("")
		{
            this.resultCode = resultCode;
		}

		public AerospikeException(string message, Exception e) 
            : base(message, e)
		{
		}

		public AerospikeException(string message) 
            : base(message)
		{
		}

        public AerospikeException(Exception e)
            : base(e.Message, e)
		{
		}

		public AerospikeException()
			: base("")
		{
		}

		public override string Message
		{
			get
			{
				StringBuilder sb = new StringBuilder();
				string message = base.Message;
    
				if (resultCode != 0)
				{
					sb.Append("Error Code ");
					sb.Append(resultCode);
					sb.Append(": ");
    
					if (message != null && message.Length > 0)
					{
						sb.Append(message);
					}
					else
					{
						sb.Append(ResultCode.GetResultString(resultCode));
					}
				}
				else
				{
					if (message != null)
					{
						sb.Append(message);
					}
					else
					{
						sb.Append(this.GetType().FullName);
					}
				}
				return sb.ToString();
			}
		}

		/// <summary>
		/// Get integer result code.
		/// </summary>
		public int Result
		{
			get
			{
				return resultCode;
			}
		}

		/// <summary>
		/// Exception thrown when database request expires before completing.
		/// </summary>
		public sealed class Timeout : AerospikeException
		{
			public Timeout() : base(ResultCode.TIMEOUT)
			{
			}
		}

		/// <summary>
		/// Exception thrown when Java serialization error occurs.
		/// </summary>
		public sealed class Serialize : AerospikeException
		{
			public Serialize(Exception e) : base(ResultCode.SERIALIZE_ERROR, e)
			{
			}
			public Serialize(string message) : base(ResultCode.SERIALIZE_ERROR, message)
			{
			}
		}

		/// <summary>
		/// Exception thrown when client can't parse data returned from server.
		/// </summary>
		public sealed class Parse : AerospikeException
		{
			public Parse(string message) : base(ResultCode.PARSE_ERROR, message)
			{
			}
		}

		/// <summary>
		/// Exception thrown when client can't connect to the server.
		/// </summary>
		public sealed class Connection : AerospikeException
		{
			public Connection(string message) : base(ResultCode.SERVER_NOT_AVAILABLE, message)
			{
			}

			public Connection(Exception e) : base(ResultCode.SERVER_NOT_AVAILABLE, e)
			{
			}
		}

		/// <summary>
		/// Exception thrown when chosen node is not active.
		/// </summary>
		public sealed class InvalidNode : AerospikeException
		{
			public InvalidNode() : base(ResultCode.INVALID_NODE_ERROR)
			{
			}
		}

		/// <summary>
		/// Exception thrown when scan was terminated prematurely.
		/// </summary>
		public sealed class ScanTerminated : AerospikeException
		{
			public ScanTerminated() : base(ResultCode.SCAN_TERMINATED)
			{
			}

			public ScanTerminated(Exception e) : base(ResultCode.SCAN_TERMINATED, e)
			{
			}
		}

		/// <summary>
		/// Exception thrown when query was terminated prematurely.
		/// </summary>
		public sealed class QueryTerminated : AerospikeException
		{
			public QueryTerminated() : base(ResultCode.QUERY_TERMINATED)
			{
			}

			public QueryTerminated(Exception e) : base(ResultCode.QUERY_TERMINATED, e)
			{
			}
		}

		/// <summary>
		/// Exception thrown when asynchronous command was rejected because the 
		/// max concurrent database commands have been exceeded.
		/// </summary>
		public sealed class CommandRejected : AerospikeException
		{
			public CommandRejected() : base(ResultCode.COMMAND_REJECTED)
			{
			}
		}
	}
}