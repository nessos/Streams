using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FsCheck;
using NUnit.Framework;
using Nessos.Streams.CSharp;

namespace Nessos.Streams.Tests.CSharp
{
    [TestFixture]
    [Category("ParStreams.CSharp")]
    public class ParStreamsTests
    {

        [Test]
        public void OfArray()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OfList()
        {
            Prop.ForAll<List<int>>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).ToList();
                var y = xs.AsParallel().Select(i => i + 1).ToList();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OfEnumerable()
        {
            Prop.ForAll<List<int>>(xs =>
            {
                IEnumerable<int> _xs = xs;
                var x = _xs.AsParStream().Select(i => i + 1).ToArray();
                var y = _xs.AsParallel().Select(i => i + 1).ToArray();
                return new SortedSet<int>(x).SequenceEqual(new SortedSet<int>(y));
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Select()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Where()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Where(i => i % 2 == 0).ToArray();
                var y = xs.AsParallel().Where(i => i % 2 == 0).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void SelectMany()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().SelectMany(i => xs.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany(i => xs).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }


        [Test]
        public void Aggregate()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right);
                var y = xs.AsParallel().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right, i => i);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }


        [Test]
        public void Sum()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).Sum();
                var y = xs.AsParallel().Select(i => i + 1).Sum();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Count()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).Count();
                var y = xs.AsParallel().Select(i => i + 1).Count();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OrderBy()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Select(i => i + 1).OrderBy(i => i).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).OrderBy(i => i).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void GroupBy()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream()
                          .Select(i => i + 1)
                          .GroupBy(i => i)
                          .Select(grouping => grouping.Count())
                          .ToArray();
                var y = xs
                        .AsParallel()
                        .Select(i => i + 1)
                        .GroupBy(i => i)
                        .Select(grouping => grouping.Count())
                        .ToArray();
                return new SortedSet<int>(x).SequenceEqual(new SortedSet<int>(y));
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void FirstWithPredicate()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = 0;
                try
                {
                    x = xs.AsParStream().First(i => i == 0);
                }
                catch (InvalidOperationException)
                {
                    x = -1;
                }
                var y = 0;
                try
                {
                    y = xs.AsParallel().First(i => i == 0);
                }
                catch (InvalidOperationException)
                {
                    y = -1;
                }
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void First()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = 0;
                try
                {
                    x = xs.AsParStream().First();
                }
                catch (InvalidOperationException)
                {
                    x = -1;
                }
                var y = 0;
                try
                {
                    y = xs.AsParallel().First();
                }
                catch (InvalidOperationException)
                {
                    y = -1;
                }
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void FirstOrDefault()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Where(_x => _x == 1).FirstOrDefault();
                var y = xs.AsParallel().Where(_x => _x == 1).FirstOrDefault();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void FirstOrDefaultWithPredicate()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().FirstOrDefault(i => i == 1);
                var y = xs.AsParallel().FirstOrDefault(i => i == 1);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        public void Any()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().Any(i => i % 2 == 0);
                var y = xs.AsParallel().Any(i => i % 2 == 0);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        public void All()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsParStream().All(i => i % 2 == 0);
                var y = xs.AsParallel().All(i => i % 2 == 0);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        public void IsEmpty()
        {
            Prop.ForAll<int[]>((int[] xs) =>
            {
                var x = xs.AsParStream().IsEmpty<int>();
                var y = xs.AsStream().IsEmpty<int>();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }
    }
}
