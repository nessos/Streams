namespace Nessos.Streams
open System
open System.Collections.Generic
open System.Linq
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Nessos.Streams.Internals

/// Represents the iteration function
type IterFunc<'T> = {
    /// The index of the current element
    Index : int ref 
    /// The composed continutation with 'T for the current value and bool is a flag for early termination
    Func : ('T -> bool)
}

/// Collects elements into a mutable result container.
type Collector<'T, 'R> = 
    /// Gets an iterator over the elements.
    abstract Iterator : unit -> IterFunc<'T>
    /// The result of the collector.
    abstract Result : 'R

/// Represents a parallel Stream of values.
type ParStream<'T> =
    /// A flag that indicates that the ordering in the subsequent query operators will be preserved.
    abstract PreserveOrdering : bool
    /// Applies the given collector to the parallel Stream.
    abstract Apply<'R> : Collector<'T, 'R> -> unit

/// Provides basic operations on Parallel Streams.
[<RequireQualifiedAccessAttribute>]
module ParStream =

    let private totalWorkers = Environment.ProcessorCount

    let private getPartitions length =
        [| 
            for i in 0 .. totalWorkers - 1 ->
                let i, j = length * i / totalWorkers, length * (i + 1) / totalWorkers in (i, j) 
        |]

    // generator functions

    /// <summary>Wraps array as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    let ofArray (source : 'T []) : ParStream<'T> =
        { new ParStream<'T> with
            member self.PreserveOrdering = false
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                if not (source.Length = 0) then 
                    let partitions = getPartitions source.Length
                    let nextRef = ref true
                    let createTask s e (iter : IterFunc<'T>) = 
                        Task.Factory.StartNew(fun () ->
                                                let mutable i = s
                                                while i < e && !nextRef do
                                                    iter.Index := i
                                                    if not <| iter.Func source.[i] then
                                                        nextRef := false
                                                    i <- i + 1 
                                                ())
                    let tasks = partitions |> Array.map (fun (s, e) -> 
                                                            let iter = collector.Iterator()
                                                            createTask s e iter)

                    Task.WaitAll(tasks) }

    /// <summary>Wraps ResizeArray as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    let ofResizeArray (source : ResizeArray<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.PreserveOrdering = false
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                if not (source.Count = 0) then 
                    let partitions = getPartitions source.Count
                    let nextRef = ref true
                    let createTask s e (iter : IterFunc<'T>) = 
                        Task.Factory.StartNew(fun () ->
                                                let mutable i = s
                                                while i < e && !nextRef do
                                                    iter.Index := i
                                                    if not <| iter.Func source.[i] then
                                                        nextRef := false
                                                    i <- i + 1 
                                                ())
                    let tasks = partitions |> Array.map (fun (s, e) -> 
                                                            let iter = collector.Iterator()
                                                            createTask s e iter)

                    Task.WaitAll(tasks) }

    /// <summary>Wraps seq as a parallel stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result parallel stream.</returns>
    let ofSeq (source : seq<'T>) : ParStream<'T> =
        match source with
        | :? ('T[]) as array -> ofArray array
        | :? ResizeArray<'T> as list -> ofResizeArray list
        | _ ->
            { new ParStream<'T> with
                member self.PreserveOrdering = false
                member self.Apply<'R> (collector : Collector<'T, 'R>) =
                
                    let partitioner = Partitioner.Create(source)
                    let partitions = partitioner.GetOrderablePartitions(totalWorkers).ToArray()
                    let nextRef = ref true
                    let createTask (partition : IEnumerator<KeyValuePair<int64, 'T>>) (iter : IterFunc<'T>) = 
                        Task.Factory.StartNew(fun () ->
                                                while partition.MoveNext() && !nextRef do
                                                    iter.Index := int partition.Current.Key
                                                    if not <| iter.Func partition.Current.Value then
                                                        nextRef := false
                                                ())
                    let tasks = partitions |> Array.map (fun partition -> 
                                                            let iter = collector.Iterator()
                                                            createTask partition iter)

                    Task.WaitAll(tasks) }


    // intermediate functions

    /// <summary>Transforms each element of the input parallel stream.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline map (f : 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Index = index; Func = iter } = collector.Iterator()
                            { Index = index; Func = (fun value -> iter (f value)) }
                        member self.Result = collector.Result  }
                stream.Apply collector }

    /// <summary>Transforms each element of the input parallel stream. The integer index passed to the function indicates the index of element being transformed.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline mapi (f : int -> 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Index = index; Func = iter } = collector.Iterator()
                            { Index = index; Func = (fun value -> iter (f !index value)) }
                        member self.Result = collector.Result  }
                stream.Apply collector }

    /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline flatMap (f : 'T -> Stream<'R>) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Index = index; Func = iter } = collector.Iterator()
                            { Index = index;
                              Func = (fun value -> 
                                        let (Stream streamf) = f value
                                        let { Bulk = bulk; Iterator = _ } = streamf (fun () -> ()) iter in bulk (); true) }
                        member self.Result = collector.Result  }
                stream.Apply collector }

    /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline collect (f : 'T -> Stream<'R>) (stream : ParStream<'T>) : ParStream<'R> =
        flatMap f stream

    /// <summary>Filters the elements of the input parallel stream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline filter (predicate : 'T -> bool) (stream : ParStream<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Apply<'S> (collector : Collector<'T, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Index = index; Func = iter } = collector.Iterator()
                            { Index = index; Func = (fun value -> if predicate value then iter value else true) }
                        member self.Result = collector.Result }
                stream.Apply collector }

    /// <summary>Applies the given function to each element of the parallel stream and returns the parallel stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline choose (chooser : 'T -> 'R option) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Index = index; Func = iter } = collector.Iterator()
                            { Index = index; Func = (fun value -> match chooser value with Some value' -> iter value' | None -> true) }
                        member self.Result = collector.Result }
                stream.Apply collector }

    /// <summary>Returns a parallel stream that preserves ordering.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream as ordered.</returns>
    let inline ordered (stream : ParStream<'T>) : ParStream<'T> = 
        { new ParStream<'T> with
                member self.PreserveOrdering = true
                member self.Apply<'S> (collector : Collector<'T, 'S>) =
                    let collector = 
                        { new Collector<'T, 'S> with
                            member self.Iterator() = 
                                let { Index = index; Func = iter } = collector.Iterator()
                                { Index = index; Func = (fun value -> iter value) }
                            member self.Result = collector.Result }
                    stream.Apply collector }

    /// <summary>Returns a parallel stream that is unordered.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream as unordered.</returns>
    let inline unordered (stream : ParStream<'T>) : ParStream<'T> = 
        { new ParStream<'T> with
                member self.PreserveOrdering = false
                member self.Apply<'S> (collector : Collector<'T, 'S>) =
                    let collector = 
                        { new Collector<'T, 'S> with
                            member self.Iterator() = 
                                let { Index = index; Func = iter } = collector.Iterator()
                                { Index = index; Func = (fun value -> iter value) }
                            member self.Result = collector.Result }
                    stream.Apply collector }

    // terminal functions

    /// <summary>Applies the given function to each element of the parallel stream.</summary>
    /// <param name="f">A function to apply to each element of the parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>    
    let inline iter (f : 'T -> unit) (stream : ParStream<'T>) : unit = 
        let collector = 
            { new Collector<'T, 'State> with
                member self.Iterator() = 
                    { Index = ref -1; Func = (fun value -> f value; true) }
                member self.Result = 
                    raise <| System.InvalidOperationException()  }
        stream.Apply collector

    /// <summary>Applies a function to each element of the parallel stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the parallel stream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : ParStream<'T>) : 'State =

        let results = new List<'State ref>()
        let collector = 
            { new Collector<'T, 'State> with
                member self.Iterator() = 
                    let accRef = ref <| state ()
                    results.Add(accRef)
                    { Index = ref -1; Func = (fun value -> accRef := folder !accRef value; true) }
                member self.Result = 
                    let mutable acc = state ()
                    for result in results do
                         acc <- combiner acc !result 
                    acc }
        stream.Apply collector
        collector.Result

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : ParStream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    /// <summary>Returns the total number of elements of the parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (stream : ParStream<'T>) : int =
        fold (fun acc _  -> 1 + acc) (+) (fun () -> 0) stream

    /// <summary>Creates an array from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (stream : ParStream<'T>) : 'T[] =
        let arrayCollector = 
            fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                (fun left right -> left.AddRange(right); left) 
                (fun () -> new ArrayCollector<'T>()) stream 
        arrayCollector.ToArray()

    /// <summary>Creates an Seq from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result Seq.</returns>    
    let inline toSeq (stream : ParStream<'T>) : seq<'T> =
        if stream.PreserveOrdering then
            toArray stream :> _
        else
            let concurrentBag = new ConcurrentBag<'T>()
            fold (fun _ value -> concurrentBag.Add(value))
                 (fun left right -> ()) 
                 (fun () -> ()) stream |> ignore
            concurrentBag :> _

    /// <summary>Creates an ResizeArray from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result ResizeArray.</returns>    
    let inline toResizeArray (stream : ParStream<'T>) : ResizeArray<'T> =
        new ResizeArray<'T>(toArray stream)


    /// <summary>Locates the maximum element of the parallel stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="source">The input parallel stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline maxBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : ParStream<'T>) : 'T =
        let result =
            fold (fun state t -> 
                let key = projection t 
                match state with 
                | None -> Some (ref t, ref key)
                | Some (refValue, refKey) when !refKey < key -> 
                    refValue := t
                    refKey := key
                    state
                | _ -> state) (fun left right -> 
                                    match left, right with
                                    | Some (_, key), Some (_, key') ->
                                        if !key' > !key then right else left
                                    | None, _ -> right
                                    | _, None -> left) (fun () -> None) source

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (refValue,_) -> !refValue

    /// <summary>Locates the minimum element of the parallel stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="source">The input parallel stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline minBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : ParStream<'T>) : 'T =
        let result = 
            fold (fun state t ->
                let key = projection t 
                match state with 
                | None -> Some (ref t, ref key)
                | Some (refValue, refKey) when !refKey > key -> 
                    refValue := t
                    refKey := key
                    state 
                | _ -> state) (fun left right -> 
                                    match left, right with
                                    | Some (_, key), Some (_, key') ->
                                        if !key' > !key then left else right
                                    | None, _ -> right
                                    | _, None -> left) (fun () -> None) source

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (refValue,_) -> !refValue


    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>    
    let inline sortBy (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'T>  =
        // explicit use of Tuple<ArrayCollector<'Key>, ArrayCollector<'T>> to avoid temp heap allocations of (ArrayCollector<'Key> * ArrayCollector<'T>) 
        let keyValueTuple = 
            fold (fun (keyValueTuple : Tuple<ArrayCollector<'Key>, ArrayCollector<'T>>) value -> 
                    let keyArray, valueArray = keyValueTuple.Item1, keyValueTuple.Item2
                    keyArray.Add(projection value)
                    valueArray.Add(value) 
                    keyValueTuple)
                (fun leftKeyValueTuple rightKeyValueTuple ->
                    let leftKeyArray, leftValueArray = leftKeyValueTuple.Item1, leftKeyValueTuple.Item2 
                    let rightKeyArray, rightValueArray = rightKeyValueTuple.Item1, rightKeyValueTuple.Item2 
                    leftKeyArray.AddRange(rightKeyArray)
                    leftValueArray.AddRange(rightValueArray)
                    leftKeyValueTuple) 
                (fun () -> new Tuple<_, _>(new ArrayCollector<'Key>(), new ArrayCollector<'T>())) stream 
        let keyArray, valueArray = keyValueTuple.Item1, keyValueTuple.Item2
        let keyArray' = keyArray.ToArray()
        let valueArray' = valueArray.ToArray()
        Sort.parallelSort keyArray' valueArray'
        valueArray' |> ofArray |> ordered

    /// <summary>Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input ParStream to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the ParStream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input ParStream.</param>
    /// <returns>The final result.</returns>
    let inline foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (combiner : 'State -> 'State -> 'State) 
                      (state : unit -> 'State) (stream : ParStream<'T>) : ParStream<'Key * 'State> =
        let dict = new ConcurrentDictionary<'Key, 'State ref>()
        let collector = 
            { new Collector<'T,  Object> with
                member self.Iterator() = 
                    { Index = ref -1; Func =
                        (fun value -> 
                                let mutable grouping = Unchecked.defaultof<_>
                                let key = projection value
                                if dict.TryGetValue(key, &grouping) then
                                    let acc = grouping
                                    lock grouping (fun () -> acc := folder !acc value) 
                                else
                                    grouping <- ref <| state ()
                                    if not <| dict.TryAdd(key, grouping) then
                                        dict.TryGetValue(key, &grouping) |> ignore
                                    let acc = grouping
                                    lock grouping (fun () -> acc := folder !acc value) 
                                true) }
                member self.Result = 
                    raise <| System.InvalidOperationException() }
        stream.Apply collector
        dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, !keyValue.Value))
        

    /// <summary>
    /// Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input ParStream to keys.</param>
    /// <param name="stream">The input ParStream.</param>
    let inline countBy (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'Key * int> =
        foldBy projection (fun state _ -> state + 1) (+) (fun () -> 0) stream

    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>A parallel stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline groupBy (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'Key * seq<'T>> =
        let dict = new ConcurrentDictionary<'Key, List<'T>>()
        
        let collector = 
            { new Collector<'T, Object> with
                member self.Iterator() = 
                    { Index = ref -1; Func =
                        (fun value -> 
                            let mutable grouping = Unchecked.defaultof<List<'T>>
                            let key = projection value
                            if dict.TryGetValue(key, &grouping) then
                                let list = grouping
                                lock grouping (fun () -> list.Add(value))
                            else
                                grouping <- new List<'T>()
                                if not <| dict.TryAdd(key, grouping) then
                                    dict.TryGetValue(key, &grouping) |> ignore
                                let list = grouping
                                lock grouping (fun () -> list.Add(value))     
                            true) }
                member self.Result = 
                    raise <| System.InvalidOperationException() }
        stream.Apply collector

        dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value :> _))

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (stream : ParStream<'T>) : 'T option = 
        let resultRef = ref Unchecked.defaultof<'T option>
        let collector = 
            { new Collector<'T, 'T option> with
                member self.Iterator() = 
                    { Index = ref -1; Func = (fun value -> if predicate value then resultRef := Some value; false else true) }
                member self.Result = 
                    !resultRef }
        stream.Apply collector
        collector.Result

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the parallel stream.</exception>
    let inline find (predicate : 'T -> bool) (stream : ParStream<'T>) : 'T = 
        match tryFind predicate stream with
        | Some value -> value
        | None -> raise <| new KeyNotFoundException()

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPick (chooser : 'T -> 'R option) (stream : ParStream<'T>) : 'R option = 
        let resultRef = ref Unchecked.defaultof<'R option>
        let collector = 
            { new Collector<'T, 'R option> with
                member self.Iterator() = 
                    { Index = ref -1; Func = (fun value -> match chooser value with Some value' -> resultRef := Some value'; false | None -> true) }
                member self.Result = 
                    !resultRef }
        stream.Apply collector
        collector.Result

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the parallel stream evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input paralle stream.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the parallel stream evaluates to None when the given function is applied.</exception>
    let inline pick (chooser : 'T -> 'R option) (stream : ParStream<'T>) : 'R = 
        match tryPick chooser stream with
        | Some value -> value
        | None -> raise <| new KeyNotFoundException()

    /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline exists (predicate : 'T -> bool) (stream : ParStream<'T>) : bool = 
        match tryFind predicate stream with
        | Some value -> true
        | None -> false


    /// <summary>Tests if all elements of the parallel stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forall (predicate : 'T -> bool) (stream : ParStream<'T>) : bool = 
        not <| exists (fun x -> not <| predicate x) stream



    /// <summary>Returns the elements of the parallel stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result prallel stream.</returns>
    let inline take (n : int) (stream : ParStream<'T>) : ParStream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        if stream.PreserveOrdering then
            let array = stream |> toArray 
            array.Take(n).ToArray() |> ofArray
        else
            { new ParStream<'T> with
                member self.PreserveOrdering = stream.PreserveOrdering
                member self.Apply<'S> (collector : Collector<'T, 'S>) =
                    let collector = 
                        let count = ref 0
                        { new Collector<'T, 'S> with
                            member self.Iterator() = 
                                let { Index = index; Func = iter } = collector.Iterator()
                                { Index = index; Func = (fun value -> if Interlocked.Increment count <= n then iter value else false) }
                            member self.Result = collector.Result }
                    stream.Apply collector }

    /// <summary>Returns a parallel stream that skips N elements of the input parallel stream and then yields the remaining elements of the stream.</summary>
    /// <param name="n">The number of items to skip.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline skip (n : int) (stream : ParStream<'T>) : ParStream<'T> =
        if stream.PreserveOrdering then
            let array = stream |> toArray 
            array.Skip(n).ToArray() |> ofArray
        else
            { new ParStream<'T> with
                member self.PreserveOrdering = stream.PreserveOrdering
                member self.Apply<'S> (collector : Collector<'T, 'S>) =
                    let collector = 
                        let count = ref 0
                        { new Collector<'T, 'S> with
                            member self.Iterator() = 
                                let { Index = index; Func = iter } = collector.Iterator()
                                { Index = index; Func = (fun value -> if Interlocked.Increment count > n then iter value else true) }
                            member self.Result = collector.Result }
                    stream.Apply collector }



