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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Aerospike.Demo
{
	public abstract class Example
	{
		protected internal Console console;
        private readonly ManualResetEventSlim manualResetEvent = new ManualResetEventSlim();

        public WaitHandle WaitHandle
        {
            get
            {
                return manualResetEvent.WaitHandle;
            }
        }

        public bool Valid
        {
            get
            {
                return !manualResetEvent.IsSet;
            }
            set
            {
                if (value)
                {
                    manualResetEvent.Reset();
                }
                else
                {
                    manualResetEvent.Set();
                }
            }
        }

		public Example(Console console)
		{
			this.console = console;
		}

		public void Stop()
		{
			Valid = false;
		}

		public void Run(Arguments args)
		{
			Valid = true;
			console.Clear();
			console.Info(this.GetType().Name + " Begin");
			RunExample(args);
			console.Info(this.GetType().Name + " End");
		}

		public abstract void RunExample(Arguments args);
	}
}
