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

        /// <summary>Creates an IEnumerable from the given stream.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result IEnumerable.</returns>    
        public static IEnumerable<TSource> ToEnumerable<TSource>(this Stream<TSource> stream)
        {
            return Stream.toSeq(stream);
        }

        /// <summary>Creates an ResizeArray from the given stream.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The result ResizeArray.</returns>    
        public static List<TSource> ToList<TSource>(this Stream<TSource> stream)
        {
            return Stream.toResizeArray(stream);
        }

        /// <summary>Returns the first element for which the given function returns true. Raises InvalidOperationException if no such element exists.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The first element for which the predicate returns true.</returns>
        /// <exception cref="System.InvalidOperationException">Thrown if the predicate evaluates to false for all the elements of the stream or the stream is empty.</exception>
        public static TSource First<TSource>(this Stream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.First(stream, predicate);
        }

        /// <summary>Returns the first element in the stream.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The first element in the stream.</returns>
        /// <exception cref="System.InvalidOperationException">Thrown if the stream is empty.</exception>
        public static T First<T>(this Stream<T> stream)
        {
            return CSharpProxy.First(stream);
        }

        /// <summary>Returns the first element in the stream, or the default value if the stream is empty.</summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>The first element in the stream, or the default value if the input stream is empty.</returns>
        public static T FirstOrDefault<T>(this Stream<T> stream)
        {
            return CSharpProxy.FirstOrDefault(stream);
        }

        /// <summary>Returns the first element for which the given function returns true. Returns the default value if no such element exists, or the input stream is empty.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>The first element for which the predicate returns true, or the default value if no such element exists or the input stream is empty.</returns>
        public static T FirstOrDefault<T>(this Stream<T> stream, Func<T, bool> predicate)
        {
            return stream.Where(predicate).FirstOrDefault();
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

        /// <summary>Locates the maximum element of the stream by given key.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="source">The input stream.</param>
        /// <returns>The maximum item.</returns>  
        public static TSource MaxBy<TSource, TKey>(this Stream<TSource> source, Func<TSource, TKey> projection)
        {
            return CSharpProxy.MaxBy(source, projection);
        }

        /// <summary>Locates the minimum element of the stream by given key.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="source">The input stream.</param>
        /// <returns>The maximum item.</returns>  
        public static TSource MinBy<TSource, TKey>(this Stream<TSource> source, Func<TSource, TKey> projection)
        {
            return CSharpProxy.MinBy(source, projection);
        }

        /// <summary>Applies a state-updating function to a stream of inputs, grouped by key projection.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="folder">Folding function.</param>
        /// <param name="init">State initializing function.</param>
        /// <param name="source">The input stream.</param>
        /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
        public static Stream<Tuple<TKey, TState>> AggregateBy<TSource, TKey, TState>(this Stream<TSource> source, Func<TSource,TKey> projection, Func<TState, TSource, TState> folder, Func<TState> init)
        {
            return CSharpProxy.AggregateBy(source, projection, folder, init);
        }

        /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and their frequency.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="stream">The input stream.</param>
        /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
        public static Stream<Tuple<TKey, int>> CountBy<TSource, TKey>(this Stream<TSource> stream, Func<TSource, TKey> projection)
        {
            return CSharpProxy.CountBy(stream, projection);
        }


        /// <summary>Applies a specified function to the corresponding elements of two streams, producing a stream of the results.</summary>
        /// <param name="resultSelector">The combiner function.</param>
        /// <param name="first">The first input stream.</param>
        /// <param name="second">The second input stream.</param>
        /// <returns>The result stream.</returns>
        public static Stream<TResult> Zip<TFirst, TSecond, TResult>(this Stream<TFirst> first, Stream<TSecond> second, Func<TFirst, TSecond, TResult> resultSelector)
        {
            return CSharpProxy.Zip(first, second, resultSelector);
        }


        /// <summary>Returns true if the stream is empty, false otherwise.</summary>
        /// <param name="source">The input stream.</param>
        /// <returns>true if the input stream is empty, false otherwise.</returns>
        public static bool IsEmpty<T>(this Stream<T> source)
        {
            return CSharpProxy.IsEmpty(source);
        }


        /// <summary>
        ///    Reduces the elements of the input stream to a single value via the given reducer function.
        ///    The reducer function is first applied to the first two elements of the stream.
        ///    Then, the reducer is applied on the result of the first reduction and the third element.
        ///    The process continues until all the elements of the stream have been reduced.
        /// </summary>
        /// <param name="source">The input stream.</param>
        /// <param name="reducer">The reducer function.</param>
        /// <returns>The reduced value.</returns>
        /// <exception cref="System.ArgumentException">Thrown if the input stream is empty.</exception>
        public static T Reduce<T>(this Stream<T> source, Func<T, T, T> reducer)
        {
            return CSharpProxy.Reduce(reducer, source);
        }
    }
}
