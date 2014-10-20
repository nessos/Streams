using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams;
using Nessos.Streams.Internals.Cloud;
using Nessos.MBrace;

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
        public static CloudStream<T> AsCloudStream<T>(this T[] source)
        {
            return CloudStreamModule.ofArray<T>(source);
        }

        /// <summary>Wraps List as a CloudStream.</summary>
        /// <param name="source">The input array.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<T> AsCloudStream<T>(this ICloudArray<T> source)
        {
            return CloudStreamModule.ofCloudArray<T>(source);
        }

        ///// <summary>Wraps IEnumerable as a CloudStream.</summary>
        ///// <param name="source">The input seq.</param>
        ///// <returns>The result CloudStream.</returns>
        //public static CloudStream<T> OfCloudFiles<T>(this ICloudArray<T> [] sources, reader)
        //{
        //    return CloudStreamModule.ofCloudFiles<T>()
        //}

        /// <summary>Transforms each element of the inputCloudStream.</summary>
        /// <param name="f">A function to transform items from the inputCloudStream.</param>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TResult> Select<TSource, TResult>(this CloudStream<TSource> stream, Func<TSource, TResult> f)
        {
            return CSharpProxy.Select(stream, f);
        }

        /// <summary>Filters the elements of the inputCloudStream.</summary>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TSource> Where<TSource>(this CloudStream<TSource> stream, Func<TSource, bool> predicate)
        {
            return CSharpProxy.Where(stream, predicate);
        }


        /// <summary>Transforms each element of the inputCloudStream to a new stream and flattens its elements.</summary>
        /// <param name="f">A function to transform items from the inputCloudStream.</param>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The result CloudStream.</returns>
        public static CloudStream<TResult> SelectMany<TSource, TResult>(this CloudStream<TSource> stream, Func<TSource, Stream<TResult>> f)
        {
            return CSharpProxy.SelectMany(stream, f);
        }

        /// <summary>Applies a function to each element of theCloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="folder">A function that updates the state with each element from theCloudStream.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The final result.</returns>
        public static Cloud<TAccumulate> Aggregate<TSource, TAccumulate>(this CloudStream<TSource> stream, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.Aggregate(stream, state, folder, combiner);
        }

        /// <summary>Applies a function to each element of theCloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
        /// <param name="state">A function that produces the initial state.</param>
        /// <param name="folder">A function that updates the state with each element from theCloudStream.</param>
        /// <param name="combiner">A function that combines partial states into a new state.</param>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The final result.</returns>
        public static CloudStream<Tuple<TKey, TAccumulate>> AggregateBy<TSource, TKey, TAccumulate>(this CloudStream<TSource> stream, Func<TSource,TKey> projection, Func<TAccumulate> state, Func<TAccumulate, TSource, TAccumulate> folder, Func<TAccumulate, TAccumulate, TAccumulate> combiner)
        {
            return CSharpProxy.AggregateBy(stream, projection, state, folder, combiner);
        }

        /// <summary>Applies a key-generating function to each element of the inputCloudStream and yields a CloudStream ordered by keys.</summary>
        /// <param name="projection">A function to transform items of the inputCloudStream into comparable keys.</param>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The result CloudStream.</returns>    
        public static CloudStream<TSource> OrderBy<TSource, TKey>(this CloudStream<TSource> stream, Func<TSource, TKey> projection, int takeCount) where TKey : IComparable<TKey>
        {
            return CSharpProxy.OrderBy(stream, projection, takeCount);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<int> Sum(this CloudStream<int> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<long> Sum(this CloudStream<long> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<float> Sum(this CloudStream<float> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<double> Sum(this CloudStream<double> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the sum of the elements.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The sum of the elements.</returns>
        public static Cloud<decimal> Sum(this CloudStream<decimal> stream)
        {
            return CSharpProxy.Sum(stream);
        }

        /// <summary>Returns the total number of elements of theCloudStream.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The total number of elements.</returns>
        public static Cloud<long> Count<TSource>(this CloudStream<TSource> stream)
        {
            return CSharpProxy.Count(stream);
        }

        /// <summary>Returns the total number of elements of theCloudStream.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The total number of elements.</returns>
        public static CloudStream<Tuple<TKey, long>> CountBy<TSource, TKey>(this CloudStream<TSource> stream, Func<TSource, TKey> projection)
        {
            return CSharpProxy.CountBy(stream, projection);
        }

        /// <summary>Creates an array from the givenCloudStream.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The result array.</returns>    
        public static Cloud<TSource[]> ToArray<TSource>(this CloudStream<TSource> stream)
        {
            return CloudStreamModule.toArray(stream);
        }

        /// <summary>Creates an List from the givenCloudStream.</summary>
        /// <param name="stream">The inputCloudStream.</param>
        /// <returns>The result List.</returns>    
        public static Cloud<ICloudArray<TSource>> ToCloudArray<TSource>(this CloudStream<TSource> stream)
        {
            return CloudStreamModule.toCloudArray(stream);
        }
    }
}
