using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams;
using Nessos.Streams.Internals.Cloud;
using Nessos.MBrace;
using System.IO;

namespace Nessos.Streams.Cloud.CSharp
{

    /// <summary>
    /// CloudStream operations
    /// </summary>
    public static class CloudStream
    {
        /// <summary>Wraps array as a CloudStream.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TSource> AsCloudStream<TSource>(this TSource[] source)
        {
            return CloudStreamModule.ofArray<TSource>(source);
        }

        /// <summary>Constructs a CloudStream from a CloudArray.\.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TSource> AsCloudStream<TSource>(this ICloudArray<TSource> source)
        {
            return CloudStreamModule.ofCloudArray<TSource>(source);
        }

        /// <summary>Constructs a CloudStream from a collection of CloudFiles using the given reader.</summary>
        /// <param name="reader">A function to transform the contents of a CloudFile to an object.</param>
        /// <param name="sources">The collection of CloudFiles.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TResult> AsCloudStream<TResult>(this IEnumerable<ICloudFile> sources, Func<System.IO.Stream, Task<TResult>> reader)
        {
            return CSharpProxy.OfCloudFiles<TResult>(sources, reader);
        }

        /// <summary>
        /// Returns a cached version of the given CloudArray.
        /// </summary>
        /// <param name="source">The input CloudArray.</param>
        public static Cloud<ICloudArray<TSource>> Cache<TSource>(this ICloudArray<TSource> source)
        {
            return CloudStreamModule.cache(source);
        }

        /// <summary>Transforms each element of the input CloudStream.</summary>
        /// <param name="f">A function to transform items from the input CloudStream.</param>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TResult> Select<TSource, TResult>(this CloudStream<TSource> stream, Func<TSource, TResult> f)
        {
            return CSharpProxy.Select(stream, f);
        }

        /// <summary>Filters the elements of the input CloudStream.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TSource> Where<TSource>(this CloudStream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Where(stream, predicate);
        }


        /// <summary>Transforms each element of the input CloudStream to a new stream and flattens its elements.</summary>
        /// <param name="f">A function to transform items from the input CloudStream.</param>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TResult> SelectMany<TSource, TResult>(this CloudStream<TSource> stream, Func<TSource, Stream<TResult>> f)
        {
            return CSharpProxy.SelectMany(stream, f);
        }

        /// <summary>Applies a function to each element of the CloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The final result.</returns>
        public static Cloud<TAccumulate> Aggregate<TSource, TAccumulate>(this CloudStream<TSource> stream, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.Aggregate(stream, state, folder, combiner);
        }

        /// <summary>Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and the result of the threading an accumulator.</summary>
        /// <param name="projection">A function to transform items from the input CloudStream to keys.</param>
        /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The final result.</returns>
        public static CloudStream<Tuple<TKey, TAccumulate>> AggregateBy<TSource, TKey, TAccumulate>(this CloudStream<TSource> stream, Func<TSource,TKey> projection, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.AggregateBy(stream, projection, state, folder, combiner);
        }

        /// <summary>Applies a key-generating function to each element of the input CloudStream and yields the CloudStream of the given length, ordered by keys.</summary>
        /// <param name="projection">A function to transform items of the input CloudStream into comparable keys.</param>
        /// <param name="stream">The input CloudStream.</param>
        /// <param name="takeCount">The number of elements to return.</param>
        /// <returns>The result CloudStream.</returns>   
        public static CloudStream<TSource> OrderBy<TSource, TKey>(this CloudStream<TSource> stream, Func<TSource, TKey> projection, int takeCount) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderBy(stream, projection, takeCount);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<int> Sum(this CloudStream<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<long> Sum(this CloudStream<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<float> Sum(this CloudStream<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<double> Sum(this CloudStream<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<decimal> Sum(this CloudStream<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the total number of elements of the CloudStream.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The total number of elements.</returns>
        public static Cloud<long> Count<TSource>(this CloudStream<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }

        /// <summary>
        /// Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and their number of occurrences in the original sequence.
        /// </summary>
        /// <param name="projection">A function that maps items from the input CloudStream to keys.</param>
        /// <param name="stream">The input CloudStream.</param>
        public static CloudStream<Tuple<TKey, long>> CountBy<TSource, TKey>(this CloudStream<TSource> stream, Func<TSource, TKey> projection)
        {
            return CSharpProxy.CountBy(stream, projection);
        }

        /// <summary>Creates an array from the given CloudStream.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The result array.</returns>    
        public static Cloud<TSource[]> ToArray<TSource>(this CloudStream<TSource> stream)
        {
            return CloudStreamModule.toArray(stream);
        }

        /// <summary>Creates a CloudArray from the given CloudStream.</summary>
        /// <param name="stream">The input CloudStream.</param>
        /// <returns>The result CloudArray.</returns>    
        public static Cloud<ICloudArray<TSource>> ToCloudArray<TSource>(this CloudStream<TSource> stream)
        {
            return CloudStreamModule.toCloudArray(stream);
        }
    }
}
