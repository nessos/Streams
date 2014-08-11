using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams.Core;

namespace Nessos.Streams.Core.CSharp
{
    public static class Streams
    {

        /// <summary>Wraps array as a stream.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result stream.</returns>
        public static Stream<T> AsStream<T>(this T[] source)
        {
            return Stream.ofArray(source);
        }

        /// <summary>Wraps List as a stream.</summary>
        /// <param name="source">The input list.</param>
        /// <returns>The result stream.</returns>
        public static Stream<T> AsStream<T>(this List<T> source)
        {
            return Stream.ofResizeArray(source);
        }

        /// <summary>Wraps IEnumerable as a stream.</summary>
        /// <param name="source">The input seq.</param>
        /// <returns>The result stream.</returns>
        public static Stream<T> AsStream<T>(this IEnumerable<T> source)
        {
            return Stream.ofSeq(source);
        }

        /// <summary>Transforms each element of the input stream.</summary>
        /// <param name="f">A function to transform items from the input stream.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TResult> Select<TSource, TResult>(this Stream<TSource> stream, Func<TSource, TResult> f)
        {
            return CSharpProxy.Select(stream, f);
        }

        /// <summary>Filters the elements of the input stream.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TSource> Where<TSource>(this Stream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Where(stream, predicate);
        }


        public static int Sum(this Stream<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }
        
        public static long Sum(this Stream<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static float Sum(this Stream<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static double Sum(this Stream<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static decimal Sum(this Stream<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static int Count<TSource>(this Stream<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }
    }
}
