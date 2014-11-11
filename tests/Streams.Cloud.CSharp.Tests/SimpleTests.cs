using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FsCheck.Fluent;
using NUnit.Framework;
using Nessos.Streams;
using Nessos.Streams.CSharp;
using Nessos.Streams.Cloud.CSharp;
using Nessos.Streams.Cloud.CSharp.MBrace;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;

#pragma warning disable 618 // obsolete nunit attribute

namespace Nessos.Streams.Cloud.CSharp.Tests
{
    [Category("CloudStreams.CSharp.RunLocal")]
    public class RunLocal : CloudStreamsTests
    {
        Configuration c = new Configuration();

        internal override T Eval<T>(Nessos.MBrace.Cloud<T> c)
        {
            return MBrace.MBrace.RunLocal(c);
        }


        internal override Configuration config
        {
            get { return c; }
        }
    }

    [Category("CloudStreams.CSharp.Cluster")]
    public class Cluster : CloudStreamsTests
    {
        Runtime rt;
        Configuration c = new Configuration() { MaxNbOfTest = 5 };

        public Cluster() {  }

        string GetFileDir([CallerFilePath]string file = "") { return file; }

        [TestFixtureSetUp]
        public void SetUp()
        {
            var version = typeof(Nessos.MBrace.Cloud).Assembly.GetName().Version.ToString(3);
            var path = Path.GetDirectoryName(this.GetFileDir());
            Settings.MBracedExecutablePath = Path.Combine(path, "../../packages/MBrace.Runtime." + version + "-alpha/tools/mbraced.exe");
            rt = Runtime.InitLocal(3);

        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            rt.Kill();
        }

        internal override T Eval<T>(Nessos.MBrace.Cloud<T> c)
        {
            return rt.Run(c);
        }
        internal override Configuration config
        {
            get { return c; }
        }
    }

    [TestFixture]
    abstract public class CloudStreamsTests
    {
        abstract internal T Eval<T>(Nessos.MBrace.Cloud<T> c);
        abstract internal Configuration config { get; }

        [Test]
        public void OfArray()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }


        [Test]
        public void Select()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void Where()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Where(i => i % 2 == 0).ToArray();
                var y = xs.AsParallel().Where(i => i % 2 == 0).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void SelectMany()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().SelectMany<int,int>(i => xs.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany(i => xs).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }


        [Test]
        public void Aggregate()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right);
                var y = xs.AsParallel().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right, i => i);
                return this.Eval(x) == y;
            }).Check(config);
        }


        [Test]
        public void Sum()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Sum();
                var y = xs.AsParallel().Select(i => i + 1).Sum();
                return this.Eval(x) == y;
            }).Check(config);
        }

        [Test]
        public void Count()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Count();
                var y = xs.AsParallel().Select(i => i + 1).Count();
                return this.Eval(x) == y;
            }).Check(config);
        }

        [Test]
        public void OrderBy()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).OrderBy(i => i,10).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).OrderBy(i => i).Take(10).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void CustomObject1()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new Custom1 { Name = i.ToString(), Age = i }).ToArray();
                var y = xs.AsParallel().Select(i => new Custom1 { Name = i.ToString(), Age = i }).ToArray();
                return this.Eval(x).Zip(y, (l, r) => l.Name == r.Name && l.Age == r.Age).All(b => b == true);
            }).Check(config);
        }

        [Test]
        public void CustomObject2()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new Custom2 { Name = i.ToString(), Age = i }).ToArray();
                var y = xs.AsParallel().Select(i => new Custom2 { Name = i.ToString(), Age = i }).ToArray();
                return this.Eval(x).Zip(y, (l, r) => l.Name == r.Name && l.Age == r.Age).All(b => b == true);
            }).Check(config);
        }

        [Test]
        public void AnonymousType()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new { Value = i }).ToArray();
                var y = xs.AsParallel().Select(i => new { Value = i }).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void CapturedVariable()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var ys = Enumerable.Range(1, 10).ToArray();
                var x = xs.AsCloudStream().SelectMany<int,int>(_ => ys.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany<int,int>(_ => ys).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void ComprehensionSyntax()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = (from x1 in xs.AsCloudStream()
                         select x1 * x1).ToArray();
                var y = (from x2 in xs.AsParallel()
                         select x2 * x2).ToArray();
                return this.Eval(x).SequenceEqual(y);
            }).Check(config);
        }
    }
}
