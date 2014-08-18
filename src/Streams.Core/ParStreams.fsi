namespace Nessos.Streams.Core


type Collector<'T, 'R> = 
    abstract Iterator : unit -> ('T -> bool)
    abstract Result : 'R

type ParStream<'T> = 
    abstract Apply<'R> : Collector<'T, 'R> -> unit

/// <summary>Operations on Parallel Streams.</summary>
module ParStream =
    /// <summary>Wraps array as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    val  ofArray: source:'T [] -> ParStream<'T>

    /// <summary>Wraps ResizeArray as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    val  ofResizeArray: source:ResizeArray<'T> -> ParStream<'T>

    /// <summary>Wraps seq as a parallel stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result parallel stream.</returns>
    val  ofSeq: source:seq<'T> -> ParStream<'T>

    /// <summary>Transforms each element of the input parallel stream.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    val inline map: f: ('T -> 'R) -> stream: ParStream<'T> -> ParStream<'R> 

    /// <summary>Filters the elements of the input parallel stream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    val inline filter: predicate: ('T -> bool) -> stream : ParStream<'T> -> ParStream<'T>

    /// <summary>Applies the given function to each element of the parallel stream and returns the parallel stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    val inline choose: chooser: ('T -> 'R option) -> stream : ParStream<'T> -> ParStream<'R>

    /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    val inline collect: f: ('T -> Stream<'R>) -> stream: ParStream<'T> -> ParStream<'R> 

    /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    val inline flatMap: f: ('T -> Stream<'R>) -> stream: ParStream<'T> -> ParStream<'R> 

    /// <summary>Applies a function to each element of the parallel stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the parallel stream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The final result.</returns>
    val inline fold: folder : ('State -> 'T -> 'State) -> combiner: ('State -> 'State -> 'State) -> state: (unit -> 'State) -> stream: ParStream<'T> -> 'State 

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The sum of the elements.</returns>
    val inline sum: stream : ParStream< ^T > -> ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) 

    /// <summary>Returns the total number of elements of the parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The total number of elements.</returns>
    val inline length: stream : ParStream<'T> -> int 

    /// <summary>Applies the given function to each element of the parallel stream.</summary>
    /// <param name="f">A function to apply to each element of the parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>    
    val inline iter: f: ('T -> unit) -> stream: ParStream<'T> -> unit 

    /// <summary>Creates an array from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result array.</returns>    
    val inline toArray: stream: ParStream<'T> -> 'T[] 

    /// <summary>Creates an ResizeArray from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result ResizeArray.</returns>    
    val inline toResizeArray: stream: ParStream<'T> -> ResizeArray<'T> 

    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>    
    val inline sortBy: projection: ('T -> 'Key) -> stream: ParStream<'T> -> ParStream<'T> when 'Key :> System.IComparable<'Key>

    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>A parallel stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    val inline groupBy: projection: ('T -> 'Key) -> stream: ParStream<'T> -> ParStream<'Key * seq<'T>> when 'Key : equality


    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    val inline tryFind: predicate: ('T -> bool) -> stream : ParStream<'T> -> 'T option

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the parallel stream.</exception>
    val inline find: predicate: ('T -> bool) -> stream : ParStream<'T> -> 'T 

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    val inline tryPick: chooser: ('T -> 'R option) -> stream : ParStream<'T> -> 'R option

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the parallel stream evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input paralle stream.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the parallel stream evaluates to None when the given function is applied.</exception>
    val inline pick: choose: ('T -> 'R option) -> stream : ParStream<'T> -> 'R


    /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    val inline exists: predicate: ('T -> bool) -> stream : ParStream<'T> -> bool

    /// <summary>Tests if all elements of the parallel stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    val inline forall: predicate: ('T -> bool) -> stream : ParStream<'T> -> bool
