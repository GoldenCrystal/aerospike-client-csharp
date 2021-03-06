/* 
 * Copyright 2012-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous result notifications for put, append, prepend, add, delete and touch commands.
	/// </summary>
	public interface WriteListener
	{
		/// <summary>
		/// This method is called when an asynchronous write command completes successfully.
		/// </summary>
		/// <param name="key">unique record identifier</param>
		void OnSuccess(Key key);

		/// <summary>
		/// This method is called when an asynchronous write command fails.
		/// </summary>
		/// <param name="exception">error that occurred</param>
		void OnFailure(AerospikeException exception);
	}
}
