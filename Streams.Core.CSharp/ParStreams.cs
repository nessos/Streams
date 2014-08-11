using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams.Core;

namespace Nessos.Streams.Core.CSharp
{
    public static class ParStreams
    {
        /// <summary>Wraps array as a parallel stream.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<T> AsParStream<T>(this T[] source)
        {
            return ParStream.ofArray(source);
        }

        /// <summary>Wraps List as a parallel stream.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<T> AsParStream<T>(this List<T> source)
        {
            return ParStream.ofResizeArray(source);
        }

        /// <summary>Wraps IEnumerable as a parallel stream.</summary>
        /// <param name="source">The input seq.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<T> AsStream<T>(this IEnumerable<T> source)
        {
            return ParStream.ofSeq(source);
        }

        /// <summary>Transforms each element of the input parallel stream.</summary>
        /// <param name="f">A function to transform items from the input parallel stream.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<TResult> Select<TSource, TResult>(this ParStream<TSource> stream, Func<TSource, TResult> f)
        {
            return CSharpProxy.Select(stream, f);
        }

        /// <summary>Filters the elements of the input parallel stream.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<TSource> Where<TSource>(this ParStream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Where(stream, predicate);
        }


        public static int Sum(this ParStream<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }
        
        public static long Sum(this ParStream<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static float Sum(this ParStream<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static double Sum(this ParStream<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static decimal Sum(this ParStream<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        public static int Count<TSource>(this ParStream<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }
    }
}
