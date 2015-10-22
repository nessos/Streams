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
    /// Parallel Stream operations
    /// </summary>
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
        public static ParStream<T> AsParStream<T>(this IEnumerable<T> source)
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


        /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
        /// <param name="f">A function to transform items from the input parallel stream.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<TResult> SelectMany<TSource, TResult>(this ParStream<TSource> stream, Func<TSource, Stream<TResult>> f)
        {
            return CSharpProxy.SelectMany(stream, f);
        }

        /// <summary>Applies a function to each element of the parallel stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="folder">A function that updates the state with each element from the parallel stream.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The final result.</returns>
        public static TAccumulate Aggregate<TSource, TAccumulate>(this ParStream<TSource> stream, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.Aggregate(stream, state, folder, combiner);
        }

        /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered by keys.</summary>
        /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>    
        public static ParStream<TSource> OrderBy<TSource, TKey>(this ParStream<TSource> stream, Func<TSource, TKey> projection) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderBy(stream, projection);
        }


        /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered by keys in descending order.</summary>
        /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>    
        public static ParStream<TSource> OrderByDescending<TSource, TKey>(this ParStream<TSource> stream, Func<TSource, TKey> projection) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderByDescending(stream, projection);
        }

        /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered using the given comparer for the keys.</summary>
        /// <param name="projection">A function to transform items of the input parallel stream into keys.</param>
        /// <param name="comparer">A comparer for the keys.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<TSource> OrderBy<TSource, TKey>(this ParStream<TSource> stream, Func<TSource, TKey> projection, IComparer<TKey> comparer) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderBy(stream, projection, comparer);
        }


        /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream of unique keys and a sequence of all elements that have each key.</summary>
        /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>A parallel stream of IGrouping.</returns>    
        public static ParStream<System.Linq.IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this ParStream<TSource> stream, Func<TSource, TKey> projection)
        {
            return CSharpProxy.GroupBy(stream, projection);
        }


        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static int Sum(this ParStream<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static long Sum(this ParStream<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static float Sum(this ParStream<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static double Sum(this ParStream<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The sum of the elements.</returns>
        public static decimal Sum(this ParStream<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the total number of elements of the parallel stream.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The total number of elements.</returns>
        public static int Count<TSource>(this ParStream<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }

        /// <summary>Creates an array from the given parallel stream.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result array.</returns>    
        public static TSource[] ToArray<TSource>(this ParStream<TSource> stream)
        {
            return ParStream.toArray(stream);
        }

        /// <summary>Creates an List from the given parallel stream.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result List.</returns>    
        public static List<TSource> ToList<TSource>(this ParStream<TSource> stream)
        {
            return ParStream.toResizeArray(stream);
        }


        /// <summary>Returns the first element for which the given function returns true. Raises InvalidOperationException if no such element exists.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The first element for which the predicate returns true.</returns>
        /// <exception cref="System.InvalidOperationException">Thrown if the predicate evaluates to false for all the elements of the parallel stream or if the parallel stream is empty.</exception>
        public static TSource First<TSource>(this ParStream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.First(stream, predicate);
        }

        /// <summary>Returns the first element in the stream.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The first element in the parllel stream.</returns>
        /// <exception cref="System.InvalidOperationException">Thrown if the parallel stream is empty.</exception>
        public static T First<T>(this ParStream<T> stream)
        {
            return CSharpProxy.First(stream);
        }

        /// <summary>Returns the first element in the stream, or the default value if the stream is empty.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The first element in the  parallel stream, or the default value if the parallel stream is empty.</returns>
        public static T FirstOrDefault<T>(this ParStream<T> stream)
        {
            return CSharpProxy.FirstOrDefault(stream);
        }

        /// <summary>Returns the first element for which the given function returns true. Returns the default value if no such element exists, or the input stream is empty.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The first element for which the predicate returns true, or the default value if no such element exists or the input parallel stream is empty.</returns>
        public static T FirstOrDefault<T>(this ParStream<T> stream, Func<T, bool> predicate)
        {
            return stream.Where(predicate).FirstOrDefault();
        }

        /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
        public static bool Any<TSource>(this ParStream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Any(stream, predicate);
        }

        /// <summary>Tests if all elements of the parallel stream satisfy the given predicate.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
        public static bool All<TSource>(this ParStream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.All(stream, predicate);
        }


        /// <summary>Returns the elements of the parallel stream up to a specified count.</summary>
        /// <param name="n">The number of items to take.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<TSource> Take<TSource>(this ParStream<TSource> stream, int n)
        {
            return ParStream.take(n, stream);
        }

        /// <summary>Returns a stream that skips N elements of the input parallel stream and then yields the remaining elements of the stream.</summary>
        /// <param name="n">The number of items to skip.</param>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream.</returns>
        public static ParStream<TSource> Skip<TSource>(this ParStream<TSource> stream, int n)
        {
            return ParStream.skip(n, stream);
        }

        /// <summary>Locates the maximum element of the parallel stream by given key.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="source">The input stream.</param>
        /// <returns>The maximum item.</returns>  
        public static TSource MaxBy<TSource, TKey>(this ParStream<TSource> source, Func<TSource, TKey> projection)
        {
            return CSharpProxy.MaxBy(source, projection);
        }

        /// <summary>Locates the minimum element of the parallel stream by given key.</summary>
        /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
        /// <param name="source">The input stream.</param>
        /// <returns>The maximum item.</returns>  
        public static TSource MinBy<TSource, TKey>(this ParStream<TSource> source, Func<TSource, TKey> projection)
        {
            return CSharpProxy.MinBy(source, projection);
        }

        /// <summary>Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and the result of the threading an accumulator.</summary>
        /// <param name="projection">A function to transform items from the input ParStream to keys.</param>
        /// <param name="folder">A function that updates the state with each element from the ParStream.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="stream">The input ParStream.</param>
        /// <returns>The final result.</returns> 
        public static ParStream<Tuple<TKey, TState>> AggregateBy<TSource, TKey, TState>(this ParStream<TSource> stream, Func<TSource, TKey> projection, Func<TState, TSource, TState> folder, Func<TState> state)
        {
            return CSharpProxy.AggregateBy(stream, projection, folder, state);
        }

        /// <summary>
        /// Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and their number of occurrences in the original sequence.
        /// </summary>
        /// <param name="projection">A function that maps items from the input ParStream to keys.</param>
        /// <param name="stream">The input ParStream.</param>
        public static ParStream<Tuple<TKey, int>> CountBy<TSource, TKey>(this ParStream<TSource> stream, Func<TSource, TKey> projection)
        {
            return CSharpProxy.CountBy(stream, projection);
        }

        /// <summary>Returns a parallel stream that preserves ordering.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream as ordered.</returns>
        public static ParStream<TSource> AsOrdered<TSource>(this ParStream<TSource> stream)
        {
            return ParStream.ordered(stream);
        }

        /// <summary>Returns a parallel stream that is unordered.</summary>
        /// <param name="stream">The input parallel stream.</param>
        /// <returns>The result parallel stream as unordered.</returns>
        public static ParStream<TSource> AsUnordered<TSource>(this ParStream<TSource> stream)
        {
            return ParStream.unordered(stream);
        }

        /// <summary>Returns true if the stream is empty, false otherwise.</summary>
        /// <param name="source">The input stream.</param>
        /// <returns>true if the input stream is empty, false otherwise.</returns>
        public static bool IsEmpty<T>(this ParStream<T> source)
        {
            return CSharpProxy.IsEmpty(source);
        }
    }
}
