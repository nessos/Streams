namespace Nessos.Streams.Core

/// <summary>Operations on Streams.</summary>
module Stream =
    type Size = int option
    type Stream<'T> = Stream of (('T -> bool) -> unit) * Size

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

    /// <summary>Applies a key-generating function to each element of the input stream and yields an array ordered by keys. The keys are compared using generic comparison as implemented by Operators.compare.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>The result array.</returns>    
    val inline sortBy: projection: ('T -> 'Key) -> stream: Stream<'T> -> 'T [] when 'Key : comparison

    /// <summary>Applies a key-generating function to each element of the input stream and yields a sequence of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input stream into comparable keys.</param>
    /// <param name="stream">The input stream.</param>
    /// <returns>A sequence of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    val inline groupBy: projection: ('T -> 'Key) -> stream: Stream<'T> -> seq<'Key * seq<'T>> when 'Key : equality