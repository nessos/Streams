using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Nessos.Streams.Cloud.CSharp;
using Nessos.Streams.Cloud.CSharp.MBrace;

namespace Streams.Cloud.CSharp.Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            var query =
                   (from i in Enumerable.Range(1, 100).ToArray().AsCloudStream()
                    where i % 2 == 0
                    select i * i).ToArray();

            var x = MBrace.RunLocal(query);

            var version = typeof(Nessos.MBrace.Cloud).Assembly.GetName().Version.ToString(3);
            Settings.MBracedExecutablePath = "../../../../packages/MBrace.Runtime." + version + "-alpha/tools/mbraced.exe";

            var rt = Runtime.InitLocal(3);

            var y = rt.Run(query);

            var ps = rt.CreateProcess(query);

            var z = ps.AwaitResult();
        }
    }
}
