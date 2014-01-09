﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Aerospike.Demo
{
    public class BenchmarkShared
    {
        internal LatencyManager writeLatency;
        internal int writeCount;
        internal int writeFailCount;

        internal LatencyManager readLatency;
        internal int readCount;
        internal int readFailCount;

        internal int currentKey;

        public BenchmarkShared(BenchmarkArguments args)
        {
            if (args.latency)
            {
                writeLatency = new LatencyManager(args.latencyColumns, args.latencyShift);
                readLatency = new LatencyManager(args.latencyColumns, args.latencyShift);
            }
        }
    }
}
