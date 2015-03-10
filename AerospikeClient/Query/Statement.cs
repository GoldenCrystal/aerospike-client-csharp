/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System;
using System.Reflection;

namespace Aerospike.Client
{
	/// <summary>
	/// Query statement parameters.
	/// </summary>
	public sealed class Statement
	{
		internal string ns;
		internal string setName;
		internal string indexName;
		internal string[] binNames;
		internal Filter[] filters;
		internal Assembly resourceAssembly;
		internal string resourcePath;
		internal string packageName;
		internal string functionName;
		internal Value[] functionArgs;
		internal long taskId;
		internal bool returnData;

		/// <summary>
		/// Set query namespace.
		/// </summary>
		public void SetNamespace(string ns)
		{
			this.ns = ns;
		}

		/// <summary>
		/// Set optional query setname.
		/// </summary>
		public void SetSetName(string setName)
		{
			this.setName = setName;
		}

		/// <summary>
		/// Set optional query index name.  If not set, the server
		/// will determine the index from the filter's bin name.
		/// </summary>
		public void SetIndexName(string indexName)
		{
			this.indexName = indexName;
		}

		/// <summary>
		/// Set query bin names.
		/// </summary>
		public void SetBinNames(params string[] binNames)
		{
			this.binNames = binNames;
		}

		/// <summary>
		/// Set optional query filters.
		/// Currently, only one filter is allowed by the server on a secondary index lookup.
		/// If multiple filters are necessary, see QueryFilter example for a workaround.
		/// QueryFilter demonstrates how to add additional filters in an user-defined 
		/// aggregation function. 
		/// </summary>
		public void SetFilters(params Filter[] filters)
		{
			this.filters = filters;
		}

		/// <summary>
		/// Set optional query task id.
		/// </summary>
		public void SetTaskId(long taskId)
		{
			this.taskId = taskId;
		}

		/// <summary>
		/// Set Lua aggregation function parameters for a Lua package located on the filesystem.  
		/// This function will be called on both the server and client for each selected item.
		/// </summary>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		/// <param name="returnData">whether to return data back to the client or not</param>
		internal void SetAggregateFunction(string packageName, string functionName, Value[] functionArgs, bool returnData)
		{
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
			this.returnData = returnData;
		}

		/// <summary>
		/// Set Lua aggregation function parameters for a Lua package located in an assembly resource.  
		/// This function will be called on both the server and client for each selected item.
		/// </summary>
		/// <param name="resourceAssembly">assembly where resource is located.  Current assembly can be obtained by: Assembly.GetExecutingAssembly()"</param>
		/// <param name="resourcePath">namespace path where Lua resource is located.  Example: Aerospike.Client.Resources.mypackage.lua</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		/// <param name="returnData">whether to return data back to the client or not</param>
		internal void SetAggregateFunction(Assembly resourceAssembly, string resourcePath, string packageName, string functionName, Value[] functionArgs, bool returnData)
		{
			this.resourceAssembly = resourceAssembly;
			this.resourcePath = resourcePath;
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
			this.returnData = returnData;
		}

		/// <summary>
		/// Prepare statement just prior to execution.
		/// </summary>
		internal void Prepare()
		{
			if (taskId == 0)
			{
				taskId = Environment.TickCount;
			}
		}
	}
}
