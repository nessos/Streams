namespace Nessos.Streams
open System
open System.Collections.Generic
open System.Linq
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Nessos.Streams.Internals

/// Represents the iteration function
type ParIterator<'T> = {
    /// The composed continutation with 'T for the current value
    Func : ('T -> unit)
    /// The current CancellationTokenSource
    Cts : CancellationTokenSource 
}

/// Collects elements into a mutable result container.
type Collector<'T, 'R> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int 
    /// Gets an iterator over the elements.
    abstract Iterator : unit -> ParIterator<'T>
    /// The result of the collector.
    abstract Result : 'R

/// The Type of iteration source
[<RequireQualifiedAccess>]
type SourceType = Array | ResizeArray | Seq

/// Represents a parallel Stream of values.
type ParStream<'T> =    
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int 
    /// The Type of iteration source
    abstract SourceType : SourceType
    /// A flag that indicates that the ordering in the subsequent query operators will be preserved.
    abstract PreserveOrdering: bool
    /// Returns the sequential Stream
    abstract Stream : unit -> Stream<'T>
    /// Applies the given collector to the parallel Stream.
    abstract Apply<'R> : Collector<'T, 'R> -> 'R

/// Provides basic operations on Parallel Streams.
[<RequireQualifiedAccessAttribute>]
module ParStream =

    
    let private getPartitions totalWorkers length =
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
            member self.DegreeOfParallelism = Environment.ProcessorCount
            member self.SourceType = SourceType.Array
            member self.PreserveOrdering = false
            member self.Stream () = Stream.ofArray source
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                if not (source.Length = 0) then 
                    let partitions = getPartitions collector.DegreeOfParallelism source.Length
                    let nextRef = ref true
                    let createTask s e (iter : ParIterator<'T>) =
                        iter.Cts.Token.Register(fun _ -> nextRef := false) |> ignore 
                        Task.Factory.StartNew(fun () ->
                                                let mutable i = s
                                                while i < e && !nextRef do
                                                    iter.Func source.[i]
                                                    i <- i + 1 
                                                ())
                    let tasks = partitions 
                                |> Array.map (fun (s, e) -> ((s, e), collector.Iterator()))
                                |> Array.map (fun ((s, e), iter) -> createTask s e iter)

                    Task.WaitAll(tasks)
                collector.Result }

    /// <summary>Wraps ResizeArray as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    let ofResizeArray (source : ResizeArray<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.DegreeOfParallelism = Environment.ProcessorCount
            member self.SourceType = SourceType.ResizeArray
            member self.PreserveOrdering = false
            member self.Stream () = Stream.ofResizeArray source
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                if not (source.Count = 0) then 
                    let partitions = getPartitions collector.DegreeOfParallelism source.Count
                    let nextRef = ref true
                    let createTask s e (iter : ParIterator<'T>) = 
                        iter.Cts.Token.Register(fun _ -> nextRef := false) |> ignore 
                        Task.Factory.StartNew(fun () ->
                                                let mutable i = s
                                                while i < e && !nextRef do
                                                    iter.Func source.[i]
                                                    i <- i + 1 
                                                ())
                    let tasks = partitions 
                                |> Array.map (fun (s, e) -> ((s, e), collector.Iterator()))
                                |> Array.map (fun ((s, e), iter) -> createTask s e iter)

                    Task.WaitAll(tasks)
                collector.Result }

    /// <summary>Wraps seq as a parallel stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result parallel stream.</returns>
    let ofSeq (source : seq<'T>) : ParStream<'T> =
        match source with
        | :? ('T[]) as array -> ofArray array
        | :? ResizeArray<'T> as list -> ofResizeArray list
        | _ ->
            { new ParStream<'T> with
                member self.DegreeOfParallelism = Environment.ProcessorCount
                member self.SourceType = SourceType.Seq
                member self.PreserveOrdering = false
                member self.Stream () = Stream.ofSeq source
                member self.Apply<'R> (collector : Collector<'T, 'R>) =
                    let partitioner = Partitioner.Create(source)
                    let partitions = partitioner.GetOrderablePartitions(collector.DegreeOfParallelism).ToArray()
                    let nextRef = ref true
                    let createTask (partition : IEnumerator<KeyValuePair<int64, 'T>>) (iter : ParIterator<'T>) = 
                        iter.Cts.Token.Register(fun _ -> nextRef := false) |> ignore 
                        Task.Factory.StartNew(fun () ->
                                                while partition.MoveNext() && !nextRef do
                                                    iter.Func partition.Current.Value
                                                ())
                    let tasks = partitions 
                                |> Array.map (fun partition -> (partition, collector.Iterator())) 
                                |> Array.map (fun (partition, iter) -> createTask partition iter)

                    Task.WaitAll(tasks)
                    collector.Result }

    /// <summary>
    ///  Wraps a collection of sequences as a parallel stream.
    /// </summary>
    /// <param name="sources">Input sequences</param>
    let ofSeqs (sources : seq<#seq<'T>>) =
        match Seq.toArray sources with
        | [| |] -> ofArray [||]
        | [| singleton |] -> ofSeq singleton
        | sources -> sources |> Seq.concat |> ofSeq

    // intermediate functions

    /// <summary>Returns a parallel stream with a new degree of parallelism.</summary>
    /// <param name="degreeOfParallelism">The degree of parallelism.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline withDegreeOfParallelism (degreeOfParallelism : int) (stream : ParStream<'T>) : ParStream<'T> = 
        if degreeOfParallelism < 1 then
            raise <| new ArgumentOutOfRangeException("degreeOfParallelism")
        else    
            { new ParStream<'T> with
                    member self.DegreeOfParallelism = degreeOfParallelism
                    member self.SourceType = stream.SourceType
                    member self.PreserveOrdering = stream.PreserveOrdering
                    member self.Stream () = stream.Stream() 
                    member self.Apply<'S> (collector : Collector<'T, 'S>) =
                        let collector = 
                            { new Collector<'T, 'S> with
                                member self.DegreeOfParallelism = degreeOfParallelism
                                member self.Iterator() = 
                                    collector.Iterator()
                                member self.Result = collector.Result }
                        stream.Apply collector }

    /// <summary>Returns a parallel stream that preserves ordering.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream as ordered.</returns>
    let inline ordered (stream : ParStream<'T>) : ParStream<'T> = 
        { new ParStream<'T> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.SourceType = stream.SourceType
                member self.PreserveOrdering = true
                member self.Stream () = stream.Stream() 
                member self.Apply<'S> (collector : Collector<'T, 'S>) =
                    let collector = 
                        { new Collector<'T, 'S> with
                            member self.DegreeOfParallelism = collector.DegreeOfParallelism
                            member self.Iterator() = 
                                collector.Iterator()
                            member self.Result = collector.Result }
                    stream.Apply collector }

    /// <summary>Returns a parallel stream that is unordered.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream as unordered.</returns>
    let inline unordered (stream : ParStream<'T>) : ParStream<'T> = 
        if stream.PreserveOrdering then
            stream.Stream() |> Stream.toSeq |> ofSeq |> withDegreeOfParallelism stream.DegreeOfParallelism 
        else
            stream

    /// <summary>Transforms each element of the input parallel stream.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline map (f : 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream () = stream.Stream() |> Stream.map f 
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> iter (f value));
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                stream.Apply collector }

    /// <summary>Transforms each element of the input parallel stream. The integer index passed to the function indicates the index of element being transformed.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline mapi (f : int -> 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = true
            member self.Stream () = stream.Stream() |> Stream.mapi f 
            member self.Apply<'S> (collector : Collector<'R, 'S>) = (unordered self).Apply collector }

    /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline flatMap (f : 'T -> Stream<'R>) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream () = stream.Stream() |> Stream.flatMap f
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> 
                                        let stream' = f value
                                        let cts = CancellationTokenSource.CreateLinkedTokenSource(iterator.Cts.Token)
                                        stream' |> Stream.Internals.iterCancel cts (fun v -> iter v |> ignore))
                                Cts = iterator.Cts } 
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
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream () = stream.Stream() |> Stream.filter predicate
            member self.Apply<'S> (collector : Collector<'T, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> if predicate value then iter value else ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                stream.Apply collector }

    /// <summary>Applies the given function to each element of the parallel stream and returns the parallel stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline choose (chooser : 'T -> 'R option) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream () = stream.Stream() |> Stream.choose chooser
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> match chooser value with Some value' -> iter value' | None -> ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                stream.Apply collector }

    /// <summary>Returns the elements of the parallel stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result prallel stream.</returns>
    let inline take (n : int) (stream : ParStream<'T>) : ParStream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        { new ParStream<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream() = stream.Stream() |> Stream.take n
            member self.Apply<'S> (collector : Collector<'T, 'S>) =
                let collector = 
                    let count = ref 0
                    { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> if Interlocked.Increment count <= n then iter value else iterator.Cts.Cancel());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                stream.Apply collector }

    /// <summary>Returns a parallel stream that skips N elements of the input parallel stream and then yields the remaining elements of the stream.</summary>
    /// <param name="n">The number of items to skip.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline skip (n : int) (stream : ParStream<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream() = stream.Stream() |> Stream.skip n
            member self.Apply<'S> (collector : Collector<'T, 'S>) =
                let collector = 
                    let count = ref 0
                    { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> if Interlocked.Increment count > n then iter value else ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                stream.Apply collector }

    // terminal functions

    /// <summary>Applies the given function to each element of the parallel stream.</summary>
    /// <param name="f">A function to apply to each element of the parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>    
    let inline iter (f : 'T -> unit) (stream : ParStream<'T>) : unit = 
        if stream.PreserveOrdering then 
            stream.Stream() |> Stream.iter f 
        else
            let collector = 
                { new Collector<'T, obj> with
                    member self.DegreeOfParallelism = stream.DegreeOfParallelism
                    member self.Iterator() = 
                        { Func = (fun value -> f value);
                          Cts = new CancellationTokenSource() }
                    member self.Result = 
                        () :> _ }

            stream.Apply collector |> ignore

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
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.Iterator() = 
                    let accRef = ref <| state ()
                    results.Add(accRef)
                    { Func = (fun value -> accRef := folder !accRef value);
                      Cts = new CancellationTokenSource() }
                member self.Result = 
                    let mutable acc = state ()
                    for result in results do
                         acc <- combiner acc !result 
                    acc }

        if stream.PreserveOrdering then 
            match stream.SourceType with
            | SourceType.Array | SourceType.ResizeArray -> stream.Stream() |> Stream.fold folder (state())
            | SourceType.Seq -> 
                let stream = unordered stream
                stream.Apply collector
        else 
            stream.Apply collector

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
        if stream.PreserveOrdering then
            stream.Stream() |> Stream.toArray
        else
            let arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) stream 
            arrayCollector.ToArray()

    /// <summary>Creates an ResizeArray from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result ResizeArray.</returns>    
    let inline toResizeArray (stream : ParStream<'T>) : ResizeArray<'T> =
        new ResizeArray<'T>(toArray stream)

    /// <summary>Creates an Seq from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result Seq.</returns>    
    let inline toSeq (stream : ParStream<'T>) : seq<'T> =
        match stream.SourceType, stream.PreserveOrdering with
        | SourceType.Array, false -> toArray stream :> _
        | SourceType.ResizeArray, false -> toResizeArray stream :> _
        | SourceType.Array, true -> stream.Stream() |> Stream.toArray :> _
        | SourceType.ResizeArray, true -> stream.Stream() |> Stream.toResizeArray :> _
        | SourceType.Seq, _ -> stream.Stream() |> Stream.toSeq
        

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
        if System.Environment.OSVersion.Platform = System.PlatformID.Unix then
            Array.Sort(keyArray', valueArray')
        else //Sort.parallel sort is known to cause hangs in linuxes
            Sort.parallelSort (stream.DegreeOfParallelism) keyArray' valueArray'
        valueArray' |> ofArray |> ordered |> withDegreeOfParallelism stream.DegreeOfParallelism

    /// <summary>Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input ParStream to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the ParStream.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input ParStream.</param>
    /// <returns>The final result.</returns>
    let inline foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (state : unit -> 'State) (stream : ParStream<'T>) : ParStream<'Key * 'State> =
        let dict = new ConcurrentDictionary<'Key, 'State ref>()
        let collector = 
            { new Collector<'T, ParStream<'Key * 'State>> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.Iterator() = 
                    {   Func =
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
                                    ());
                        Cts = new CancellationTokenSource()  }
                member self.Result = 
                    dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, !keyValue.Value)) |> withDegreeOfParallelism stream.DegreeOfParallelism }

        if stream.PreserveOrdering then 
            stream.Stream() |> Stream.foldBy projection folder state |> Stream.toArray |> ofArray |> withDegreeOfParallelism stream.DegreeOfParallelism 
        else 
            stream.Apply collector
        
        

    /// <summary>
    /// Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input ParStream to keys.</param>
    /// <param name="stream">The input ParStream.</param>
    let inline countBy (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'Key * int> =
        foldBy projection (fun state _ -> state + 1) (fun () -> 0) stream

    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>A parallel stream of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>    
    let inline groupBy (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'Key * seq<'T>> =
        let dict = new ConcurrentDictionary<'Key, List<'T>>()
        
        let collector = 
            { new Collector<'T, ParStream<'Key * seq<'T>>> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.Iterator() = 
                    {   Func =
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
                                ());
                        Cts = new CancellationTokenSource()   }
                member self.Result = 
                    let stream' = dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value :> seq<'T>))   
                    stream' |> withDegreeOfParallelism stream.DegreeOfParallelism }

        if stream.PreserveOrdering then 
            stream.Stream() |> Stream.groupBy projection |> Stream.toArray |> ofArray |> withDegreeOfParallelism stream.DegreeOfParallelism 
        else
            stream.Apply collector
        

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (stream : ParStream<'T>) : 'T option = 
        let resultRef = ref Unchecked.defaultof<'T option>
        let cts =  new CancellationTokenSource()
        let collector = 
            { new Collector<'T, 'T option> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                member self.Iterator() = 
                    {   Func = (fun value -> if predicate value then resultRef := Some value; cts.Cancel() else ());
                        Cts = cts }
                member self.Result = 
                    !resultRef }
        
        let stream = unordered stream
        stream.Apply collector

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
        let cts = new CancellationTokenSource()
        let collector = 
            { new Collector<'T, 'R option> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.Iterator() = 
                    {   Func = (fun value -> match chooser value with Some value' -> resultRef := Some value'; cts.Cancel() | None -> ());
                        Cts = cts }
                member self.Result = 
                    !resultRef }

        let stream = unordered stream
        stream.Apply collector


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

    /// <summary>
    ///     Returs the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream, or None if the stream has no elements.</returns>
    let inline tryHead (stream : ParStream<'T>) : 'T option =
        let r = stream |> ordered |> take 1 |> toArray
        if r.Length = 0 then None
        else Some r.[0]


    /// <summary>
    ///     Returs the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the stream has no elements.</exception>
    let inline head (stream : ParStream<'T>) : 'T =
        match tryHead stream with
        | Some value -> value
        | None -> invalidArg "stream" "The stream was empty."

    /// <summary>
    ///     Returs true if the stream is empty and false otherwise.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if the input stream is empty, false otherwise</returns>
    let inline isEmpty (stream : ParStream<'T>) : bool =
        stream |> exists (fun _ -> true) |> not
