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
    [Category("Streams.CSharp")]
    public class StreamsTests
    {

        [Test]
        public void OfArray()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OfList()
        {
            Prop.ForAll<List<int>>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).ToList();
                var y = xs.Select(i => i + 1).ToList();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OfEnumerable()
        {
            Prop.ForAll<List<int>>(xs =>
            {
                IEnumerable<int> _xs = xs;
                var x = _xs.AsStream().Select(i => i + 1).ToArray();
                var y = _xs.Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void ToEnumerable()
        {
            Prop.ForAll<List<int>>(xs =>
            {
                IEnumerable<int> _xs = xs;
                var x = _xs.AsStream().Select(i => i + 1).ToEnumerable().Count();
                var y = _xs.Select(i => i + 1).Count();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Select()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Where()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Where(i => i % 2 == 0).ToArray();
                var y = xs.Where(i => i % 2 == 0).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void SelectMany()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().SelectMany(i => xs.AsStream()).ToArray();
                var y = xs.SelectMany(i => xs).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Aggregate()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).Aggregate(0, (acc, i) => acc + i);
                var y = xs.Select(i => i + 1).Aggregate(0, (acc, i) => acc + i);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }


        [Test]
        public void Sum()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).Sum();
                var y = xs.Select(i => i + 1).Sum();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Count()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).Count();
                var y = xs.Select(i => i + 1).Count();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OrderBy()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).OrderBy(i => i).ToArray();
                var y = xs.Select(i => i + 1).OrderBy(i => i).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void GroupBy()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream()
                          .Select(i => i + 1)
                          .GroupBy(i => i)
                          .Select(grouping => grouping.Count())
                          .ToArray();
                var y = xs
                        .Select(i => i + 1)
                        .GroupBy(i => i)
                        .Select(grouping => grouping.Count())
                        .ToArray(); 
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Take()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Take(2).ToArray();
                var y = xs.Take(2).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Skip()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Skip(2).ToArray();
                var y = xs.Skip(2).ToArray();
                return x.SequenceEqual(y);
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
                    x = xs.AsStream().First(i => i % 2 == 0);
                }
                catch (InvalidOperationException)
                {
                    x = -1;
                }
                var y = 0;
                try
                {
                    y = xs.First(i => i % 2 == 0);
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
                    x = xs.AsStream().First();
                }
                catch (InvalidOperationException)
                {
                    x = -1;
                }
                var y = 0;
                try
                {
                    y = xs.First();
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
                var x = xs.AsStream().FirstOrDefault();
                var y = xs.FirstOrDefault();
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void FirstOrDefaultWithPredicate()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().FirstOrDefault(i => i % 2 == 0);
                var y = xs.FirstOrDefault(i => i % 2 == 0);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        public void Any()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().Any(i => i % 2 == 0);
                var y = xs.Any(i => i % 2 == 0);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }

        public void All()
        {
            Prop.ForAll<int[]>(xs =>
            {
                var x = xs.AsStream().All(i => i % 2 == 0);
                var y = xs.All(i => i % 2 == 0);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }


        public void Zip()
        {
            Prop.ForAll<Tuple<int[], int[]>>(tuple =>
            {
                var xs = tuple.Item1.AsStream().Zip(tuple.Item2.AsStream(), (x, y) => x + y).ToArray();
                var ys = tuple.Item1.Zip(tuple.Item2, (x, y) => x + y).ToArray();
                return xs == ys;
            }).QuickCheckThrowOnFailure();
        }

        public void IsEmpty()
        {
            Prop.ForAll<int[]>((int[] xs) =>
            {
                var x = xs.AsStream().IsEmpty<int>();
                var y = (xs.Length == 0);
                return x == y;
            }).QuickCheckThrowOnFailure();
        }
    }
}
