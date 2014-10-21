using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Nessos.Streams.Cloud.CSharp;
using Nessos.Streams.Cloud.CSharp.MBrace;

namespace Streams.Cloud.CSharp.Tests
{
    public class Person { public string Name; public int Age; }

    public class Program
    {
        static int f(int x) { return x * x; }

        public static void Main(string[] args)
        {
            var xs = Enumerable.Range(1, 100).ToArray();
            var query =
                   (from i in xs.AsCloudStream()
                    select i)
                   .ToArray();

            var x = MBrace.RunLocal(query);

            var version = typeof(Nessos.MBrace.Cloud).Assembly.GetName().Version.ToString(3);
            Settings.MBracedExecutablePath = "../../../../packages/MBrace.Runtime." + version + "-alpha/tools/mbraced.exe";

            var rt = Runtime.InitLocal(3);

            var y = rt.Run(query);

            var ps = rt.CreateProcess(query);

            var z = ps.AwaitResult();

            rt.ShowProcessInfo();

            rt.Kill();
        }
    }
}
