using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams.Cloud.CSharp;
using Nessos.Streams.Cloud.CSharp.MBrace;
using System.IO;
using System.Reflection;
using Nessos.Streams.CSharp;

namespace Nessos.Streams.Cloud.CSharp.Tests
{
    [Serializable]
    public class Person { public string Name; public int Age; }

    public class Program
    {
        static int f(int x) { return x * x; }

        public static void Main(string[] args)
        {

            var xs = Enumerable.Range(1, 10).ToArray();
            var query = xs
                        .AsCloudStream()
                        .SelectMany<int,int>(i => xs.AsStream())
                        .ToArray();

            var x = MBrace.MBrace.RunLocal(query); // wat?

            var version = typeof(Nessos.MBrace.Cloud).Assembly.GetName().Version.ToString(3);
            var path = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var mbraced = Path.Combine(path, @"..\packages\MBrace.Runtime." + version + @"-alpha\tools\mbraced.exe");
            Settings.MBracedExecutablePath = mbraced;

            var rt = Runtime.InitLocal(3);

            var y = rt.Run(query);

            var q = from l in xs.AsCloudStream()
                    where l % 2 == 0
                    select new { Name = l.ToString(), Age = f(l) };

            var ps = rt.CreateProcess(q.ToArray());

            var z = ps.AwaitResult();

            rt.ShowProcessInfo();

            rt.Kill();
        }
    }
}
