using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams;
using Nessos.Streams.Internals;

namespace Nessos.Streams.CSharp
{
    /// <summary>
    /// Stream operations
    /// </summary>
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

        /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
        /// <param name="f">A function to transform items from the input stream.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TResult> SelectMany<TSource, TResult>(this Stream<TSource> stream, Func<TSource, Stream<TResult>> f)
        {
            return CSharpProxy.SelectMany(stream, f);
        }


        /// <summary>Applies a function to each element of the stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="folder">A function that updates the state with each element from the stream.</param>
        /// <param name="state">The initial state.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The final result.</returns>
        public static TAccumulate Aggregate<TSource, TAccumulate>(this Stream<TSource> stream, TAccumulate state, Func<TAccumulate, TSource, TAccumulate> folder)
        {
            return CSharpProxy.Aggregate(stream, state, folder);
        }

        /// <summary>Applies a key-generating function to each element of the input stream and yields a stream ordered by keys. </summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TSource> OrderBy<TSource, TKey>(this Stream<TSource> stream, Func<TSource, TKey> projection) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderBy(stream, projection);
        }

        /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and a sequence of all elements that have each key.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>A stream of IGrouping.</returns>    
        public static Stream<System.Linq.IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this Stream<TSource> stream, Func<TSource, TKey> projection) 
        {
            return CSharpProxy.GroupBy(stream, projection);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static int Sum(this Stream<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static long Sum(this Stream<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static float Sum(this Stream<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static double Sum(this Stream<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static decimal Sum(this Stream<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the total number of elements of the stream.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The total number of elements.</returns>
        public static int Count<TSource>(this Stream<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }

        /// <summary>Creates an array from the given stream.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result array.</returns>    
        public static TSource[] ToArray<TSource>(this Stream<TSource> stream)
        {
            return Stream.toArray(stream);
        }

        /// <summary>Creates an ResizeArray from the given stream.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result ResizeArray.</returns>    
        public static List<TSource> ToList<TSource>(this Stream<TSource> stream)
        {
            return Stream.toResizeArray(stream);
        }

        /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The first element for which the predicate returns true.</returns>
        /// <exception cref="System.Collections.Generic.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the stream.</exception>
        public static TSource First<TSource>(this Stream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.First(stream, predicate);
        }

        /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
        public static bool Any<TSource>(this Stream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Any(stream, predicate);
        }

        /// <summary>Tests if all elements of the stream satisfy the given predicate.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
        public static bool All<TSource>(this Stream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.All(stream, predicate);
        }

        /// <summary>Returns the elements of the stream up to a specified count.</summary>
        /// <param name="n">The number of items to take.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TSource> Take<TSource>(this Stream<TSource> stream, int n)
        {
            return Stream.take(n, stream);
        }

        /// <summary>Returns a stream that skips N elements of the input stream and then yields the remaining elements of the stream.</summary>
        /// <param name="n">The number of items to skip.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TSource> Skip<TSource>(this Stream<TSource> stream, int n)
        {
            return Stream.skip(n, stream);
        }
    }
}
