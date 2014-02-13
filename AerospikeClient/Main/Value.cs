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
using System.IO;
using System.Text;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Polymorphic value classes used to efficiently serialize objects into the wire protocol.
	/// </summary>
	public abstract class Value
	{
		/// <summary>
		/// Get string or null value instance.
		/// </summary>
		public static Value Get(string value)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new StringValue(value);
			}
		}

		/// <summary>
		/// Get byte array or null value instance.
		/// </summary>
		public static Value Get(byte[] value)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new BytesValue(value);
			}
		}

		/// <summary>
		/// Get byte array segment or null value instance.
		/// </summary>
		public static Value Get(byte[] value, int offset, int length)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new ByteSegmentValue(value, offset, length);
			}
		}

		/// <summary>
		/// Get long value instance.
		/// </summary>
		public static Value Get(long value)
		{
			return new LongValue(value);
		}

		/// <summary>
		/// Get unsigned long value instance.
		/// </summary>
		public static Value Get(ulong value)
		{
			return new UnsignedLongValue(value);
		}

		/// <summary>
		/// Get integer value instance.
		/// </summary>
		public static Value Get(int value)
		{
			return new IntegerValue(value);
		}

		/// <summary>
		/// Get unsigned integer value instance.
		/// </summary>
		public static Value Get(uint value)
		{
			return new UnsignedIntegerValue(value);
		}

		/// <summary>
		/// Get short value instance.
		/// </summary>
		public static Value Get(short value)
		{
			return new ShortValue(value);
		}

		/// <summary>
		/// Get short value instance.
		/// </summary>
		public static Value Get(ushort value)
		{
			return new UnsignedShortValue(value);
		}

		/// <summary>
		/// Get boolean value instance.
		/// </summary>
		public static Value Get(bool value)
		{
			return new BooleanValue(value);
		}

		/// <summary>
		/// Get boolean value instance.
		/// </summary>
		public static Value Get(byte value)
		{
			return new ByteValue(value);
		}

		/// <summary>
		/// Get signed boolean value instance.
		/// </summary>
		public static Value Get(sbyte value)
		{
			return new SignedByteValue(value);
		}

		/// <summary>
		/// Get value array instance.
		/// </summary>
		public static Value Get(Value[] value)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new ValueArray(value);
			}
		}

		/// <summary>
		/// Get blob or null value instance.
		/// </summary>
		public static Value GetAsBlob(object value)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new BlobValue(value);
			}
		}

		/// <summary>
		/// Get list or null value instance.
		/// Support by Aerospike 3 servers only.
		/// </summary>
		public static Value GetAsList(List<object> value)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new ListValue(value);
			}
		}

		/// <summary>
		/// Get map or null value instance.
		/// Support by Aerospike 3 servers only.
		/// </summary>
		public static Value GetAsMap(Dictionary<object,object> value)
		{
			if (value == null)
			{
				return new NullValue();
			}
			else
			{
				return new MapValue(value);
			}
		}

		/// <summary>
		/// Get null value instance.
		/// </summary>
		public static Value AsNull
		{
			get
			{
				return new NullValue();
			}
		}

		/// <summary>
		/// Determine value given generic object.
		/// This is the slowest of the Value get() methods.
		/// </summary>
		public static Value Get(object value)
		{
			if (value == null)
			{
				return new NullValue();
			}

			if (value is byte[])
			{
				return new BytesValue((byte[])value);
			}

			if (value is Value)
			{
				return (Value)value;
			}

			TypeCode code = System.Type.GetTypeCode(value.GetType());

			switch (code)
			{
                case TypeCode.Empty:
					return new NullValue();

                case TypeCode.String:
					return new StringValue((string)value);

				case TypeCode.Int64:
					return new LongValue((long)value);

				case TypeCode.Int32:
					return new IntegerValue((int)value);

				case TypeCode.Int16:
					return new ShortValue((short)value);

				case TypeCode.UInt64:
					return new UnsignedLongValue((ulong)value);

				case TypeCode.UInt32:
					return new UnsignedIntegerValue((uint)value);

				case TypeCode.UInt16:
					return new UnsignedShortValue((ushort)value);

				case TypeCode.Boolean:
					return new BooleanValue((bool)value);
 
                case TypeCode.Byte:
					return new ByteValue((byte)value);

                case TypeCode.SByte:
					return new SignedByteValue((sbyte)value);

                case TypeCode.Char:
                case TypeCode.DateTime:
                case TypeCode.Double:
                case TypeCode.Single:
                default:
					return new BlobValue(value);
            }
		}

		/// <summary>
		/// Calculate number of bytes necessary to serialize the value in the wire protocol.
		/// </summary>
		public abstract int EstimateSize();

		/// <summary>
		/// Serialize the value in the wire protocol.
		/// </summary>
		public abstract int Write(byte[] buffer, int offset);

		/// <summary>
		/// Serialize the value using MessagePack.
		/// </summary>
		public abstract void Pack(Packer packer);

		/// <summary>
		/// Get wire protocol value type.
		/// </summary>
		public abstract int Type {get;}

		/// <summary>
		/// Return original value as an Object.
		/// </summary>
		public abstract object Object {get;}

		/// <summary>
		/// Empty value.
		/// </summary>
		public sealed class NullValue : Value
		{
			public override int EstimateSize()
			{
				return 0;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return 0;
			}

			public override void Pack(Packer packer)
			{
				packer.PackNil();
			}

			public override int Type
			{
				get
				{
					return ParticleType.NULL;
				}
			}

			public override object Object
			{
				get
				{
					return null;
				}
			}

			public override string ToString()
			{
				return null;
			}
		}

		/// <summary>
		/// String value.
		/// </summary>
		public sealed class StringValue : Value
		{
			private readonly string value;

			public StringValue(string value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return ByteUtil.EstimateSizeUtf8(value);
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.StringToUtf8(value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackString(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.STRING;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return value;
			}
		}

		/// <summary>
		/// Byte array value.
		/// </summary>
		public sealed class BytesValue : Value
		{
			private readonly byte[] bytes;

			public BytesValue(byte[] bytes)
			{
				this.bytes = bytes;
			}

			public override int EstimateSize()
			{
				return bytes.Length;

			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackBytes(bytes);
			}

			public override int Type
			{
				get
				{
					return ParticleType.BLOB;
				}
			}

			public override object Object
			{
				get
				{
					return bytes;
				}
			}

			public override string ToString()
			{
				return ByteUtil.BytesToHexString(bytes);
			}
		}

		/// <summary>
		/// Byte segment value.
		/// </summary>
		public sealed class ByteSegmentValue : Value
		{
			private readonly byte[] bytes;
			private readonly int offset;
			private readonly int length;

			public ByteSegmentValue(byte[] bytes, int offset, int length)
			{
				this.bytes = bytes;
				this.offset = offset;
				this.length = length;
			}

			public override int EstimateSize()
			{
				return length;

			}

			public override int Write(byte[] buffer, int targetOffset)
			{
				Array.Copy(bytes, offset, buffer, targetOffset, length);
				return length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackBytes(bytes, offset, length);
			}

			public override int Type
			{
				get
				{
					return ParticleType.BLOB;
				}
			}

			public override object Object
			{
				get
				{
					return this;
				}
			}

			public override string ToString()
			{
				return ByteUtil.BytesToHexString(bytes, offset, length);
			}

			public byte[] Bytes
			{
				get
				{
					return bytes;
				}
			}

			public int Offset
			{
				get
				{
					return offset;
				}
			}

			public int Length
			{
				get
				{
					return length;
				}
			}
		}

		/// <summary>
		/// Long value.
		/// </summary>
		public sealed class LongValue : Value
		{
			private readonly long value;

			public LongValue(long value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 8;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes((ulong)value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Unsigned long value.
		/// </summary>
		public sealed class UnsignedLongValue : Value
		{
			private readonly ulong value;

			public UnsignedLongValue(ulong value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return ((value & 0x8000000000000000) == 0)? 8 : 9;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes(value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Integer value.
		/// </summary>
		public sealed class IntegerValue : Value
		{
			private readonly int value;

			public IntegerValue(int value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 8;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes((ulong)value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Unsigned integer value.
		/// </summary>
		public sealed class UnsignedIntegerValue : Value
		{
			private readonly uint value;

			public UnsignedIntegerValue(uint value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 8;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes(value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Short value.
		/// </summary>
		public sealed class ShortValue : Value
		{
			private readonly short value;

			public ShortValue(short value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 8;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes((ulong)value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Unsigned short value.
		/// </summary>
		public sealed class UnsignedShortValue : Value
		{
			private readonly ushort value;

			public UnsignedShortValue(ushort value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 8;
			}

			public override int Write(byte[] buffer, int offset)
			{
				return ByteUtil.LongToBytes(value, buffer, offset);
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Boolean value.
		/// </summary>
		public sealed class BooleanValue : Value
		{
			private readonly bool value;

			public BooleanValue(bool value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 1;
			}

			public override int Write(byte[] buffer, int offset)
			{
				buffer[offset] = (value)? (byte)1 : (byte)0;
				return 1;
			}

			public override void Pack(Packer packer)
			{
				packer.PackBoolean(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Byte value.
		/// </summary>
		public sealed class ByteValue : Value
		{
			private readonly byte value;

			public ByteValue(byte value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 1;
			}

			public override int Write(byte[] buffer, int offset)
			{
				buffer[offset] = value;
				return 1;
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Byte value.
		/// </summary>
		public sealed class SignedByteValue : Value
		{
			private readonly sbyte value;

			public SignedByteValue(sbyte value)
			{
				this.value = value;
			}

			public override int EstimateSize()
			{
				return 1;
			}

			public override int Write(byte[] buffer, int offset)
			{
				buffer[offset] = (byte)value;
				return 1;
			}

			public override void Pack(Packer packer)
			{
				packer.PackNumber(value);
			}

			public override int Type
			{
				get
				{
					return ParticleType.INTEGER;
				}
			}

			public override object Object
			{
				get
				{
					return value;
				}
			}

			public override string ToString()
			{
				return Convert.ToString(value);
			}
		}

		/// <summary>
		/// Blob value.
		/// </summary>
		public sealed class BlobValue : Value
		{
			private readonly object obj;
			private byte[] bytes;

			public BlobValue(object obj)
			{
				this.obj = obj;
			}

			public override int EstimateSize()
			{
				using (MemoryStream ms = new MemoryStream())
				{
					Formatter.Default.Serialize(ms, obj);
					bytes = ms.ToArray();
					return bytes.Length;
				}
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackBlob(bytes);
			}

			public override int Type
			{
				get
				{
					return ParticleType.CSHARP_BLOB;
				}
			}

			public override object Object
			{
				get
				{
					return obj;
				}
			}

			public override string ToString()
			{
				return ByteUtil.BytesToHexString(bytes);
			}
		}

		/// <summary>
		/// Value array.
		/// Supported by Aerospike 3 servers only.
		/// </summary>
		public sealed class ValueArray : Value
		{
			private readonly Value[] array;
			private byte[] bytes;

			public ValueArray(Value[] array)
			{
				this.array = array;
			}

			public override int EstimateSize()
			{
				bytes = Packer.Pack(array);
				return bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackValueArray(array);
			}

			public override int Type
			{
				get
				{
					return ParticleType.LIST;
				}
			}

			public override object Object
			{
				get
				{
					return array;
				}
			}

			public override string ToString()
			{
				return Util.ArrayToString(array);
			}
		}

		/// <summary>
		/// List value.
		/// Supported by Aerospike 3 servers only.
		/// </summary>
		public sealed class ListValue : Value
		{
			internal readonly List<object> list;
			internal byte[] bytes;

			public ListValue(List<object> list)
			{
				this.list = list;
			}

			public override int EstimateSize()
			{
				bytes = Packer.Pack(list);
				return bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackList(list);
			}

			public override int Type
			{
				get
				{
					return ParticleType.LIST;
				}
			}

			public override object Object
			{
				get
				{
					return list;
				}
			}

			public override string ToString()
			{
				return list.ToString();
			}
		}

		/// <summary>
		/// Map value.
		/// Supported by Aerospike 3 servers only.
		/// </summary>
		public sealed class MapValue : Value
		{
			internal readonly Dictionary<object,object> map;
			internal byte[] bytes;

			public MapValue(Dictionary<object,object> map)
			{
				this.map = map;
			}

			public override int EstimateSize()
			{
				bytes = Packer.Pack(map);
				return bytes.Length;
			}

			public override int Write(byte[] buffer, int offset)
			{
				Array.Copy(bytes, 0, buffer, offset, bytes.Length);
				return bytes.Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackMap(map);
			}

			public override int Type
			{
				get
				{
					return ParticleType.MAP;
				}
			}

			public override object Object
			{
				get
				{
					return map;
				}
			}

			public override string ToString()
			{
				return map.ToString();
			}
		}
	}
}
