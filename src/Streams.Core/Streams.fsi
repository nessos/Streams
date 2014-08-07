namespace Nessos.Streams.Core


type Stream<'T> = Stream of (('T -> bool) -> unit)

/// <summary>Operations on Streams.</summary>
module Stream =
    /// <summary>Wraps array as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    val inline ofArray: source:'T [] -> Stream<'T>

    /// <summary>Wraps ResizeArray as a stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result stream.</returns>
    val inline ofResizeArray: source:ResizeArray<'T> -> Stream<'T>

    /// <summary>Wraps seq as a stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result stream.</returns>
    val inline ofSeq: source:seq<'T> -> Stream<'T>

    /// <summary>Transforms each element of the input stream.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline map: f: ('T -> 'R) -> stream: Stream<'T> -> Stream<'R> 

    /// <summary>Filters the elements of the input stream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline filter: predicate: ('T -> bool) -> stream : Stream<'T> -> Stream<'T>

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline collect: f: ('T -> Stream<'R>) -> stream: Stream<'T> -> Stream<'R> 

    /// <summary>Transforms each element of the input stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input stream.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline flatMap: f: ('T -> Stream<'R>) -> stream: Stream<'T> -> Stream<'R> 

    /// <summary>Returns the elements of the stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline take: n: int -> stream: Stream<'T> -> Stream<'T> 

    /// <summary>Returns the elements of the stream while the given predicate returns true.</summary>
    /// <param name="pred">The predicate function.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline takeWhile: pred : ('T -> bool) -> stream : Stream<'T> -> Stream<'T>

    /// <summary>Returns a stream that skips N elements of the input stream and then yields the remaining elements of the stream.</summary>
    /// <param name="n">The number of items to skip.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>
    val inline skip: n: int -> stream: Stream<'T> -> Stream<'T> 

    /// <summary>Applies a function to each element of the stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the stream.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The final result.</returns>
    val inline fold: folder : ('State -> 'T -> 'State) -> state: 'State -> stream: Stream<'T> -> 'State 

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The sum of the elements.</returns>
    val inline sum: stream : Stream< ^T > -> ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) 

    /// <summary>Returns the total number of elements of the stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The total number of elements.</returns>
    val inline length: stream : Stream<'T> -> int 

    /// <summary>Applies the given function to each element of the stream.</summary>
    /// <param name="f">A function to apply to each element of the stream.</param>
    /// <param name="stream">The input stream.</param>    
    val inline iter: f: ('T -> unit) -> stream: Stream<'T> -> unit 

    /// <summary>Creates an array from the given stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result array.</returns>    
    val inline toArray: stream: Stream<'T> -> 'T[] 

    /// <summary>Creates an ResizeArray from the given stream.</summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result ResizeArray.</returns>    
    val inline toResizeArray: stream: Stream<'T> -> ResizeArray<'T> 

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream ordered by keys. The keys are compared using generic comparison as implemented by Operators.compare.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result stream.</returns>    
    val inline sortBy: projection: ('T -> 'Key) -> stream: Stream<'T> -> Stream<'T> when 'Key : comparison

    /// <summary>Applies a key-generating function to each element of the input stream and yields a stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>A stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    val inline groupBy: projection: ('T -> 'Key) -> stream: Stream<'T> -> Stream<'Key * seq<'T>> when 'Key : equality

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    val inline tryFind: predicate: ('T -> bool) -> stream : Stream<'T> -> 'T option

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the stream.</exception>
    val inline find: predicate: ('T -> bool) -> stream : Stream<'T> -> 'T 

    /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    val inline exists: predicate: ('T -> bool) -> stream : Stream<'T> -> bool

    /// <summary>Tests if all elements of the stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    val inline forall: predicate: ('T -> bool) -> stream : Stream<'T> -> bool