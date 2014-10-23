using Nessos.Streams.Cloud.CSharp.MBrace;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.Streams.Cloud.CSharp.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            var version = typeof(Nessos.MBrace.Cloud).Assembly.GetName().Version.ToString(3);
            var path = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var mbraced = Path.Combine(path, @"..\packages\MBrace.Runtime." + version + @"-alpha\tools\mbraced.exe");
            Settings.MBracedExecutablePath = mbraced;

            var runtime = Runtime.InitLocal(3);

            WordCount.FilesPath = @"path to your files";
            var top1 = WordCount.RunWithCloudFiles(runtime);
            var top2 = WordCount.RunWithCloudArray(runtime);

            runtime.Kill();
        }
    }
}
