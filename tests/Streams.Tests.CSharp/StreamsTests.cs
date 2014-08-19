using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FsCheck.Fluent;
using NUnit.Framework;
using Nessos.Streams.Core.CSharp;

namespace Nessos.Streams.Tests
{
    [TestFixture]
    public class StreamsTests
    {

        [Test]
        public void OfArray()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OfList()
        {
            Spec.ForAny<List<int>>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).ToList();
                var y = xs.Select(i => i + 1).ToList();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void OfEnumerable()
        {
            Spec.ForAny<List<int>>(xs =>
            {
                IEnumerable<int> _xs = xs;
                var x = _xs.AsStream().Select(i => i + 1).ToArray();
                var y = _xs.Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Select()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void Where()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsStream().Where(i => i % 2 == 0).ToArray();
                var y = xs.Where(i => i % 2 == 0).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }

        [Test]
        public void SelectMany()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsStream().SelectMany(i => xs.AsStream()).ToArray();
                var y = xs.SelectMany(i => xs).ToArray();
                return x.SequenceEqual(y);
            }).QuickCheckThrowOnFailure();
        }
    }
}
