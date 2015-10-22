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
type ParCollector<'T, 'R> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int 
    /// Gets an iterator over the elements.
    abstract Iterator : unit -> ParIterator<'T>
    /// The result of the collector.
    abstract Result : 'R

/// The Type of iteration source
[<RequireQualifiedAccess>]
type internal SourceType = Array | FSharpList | ResizeArray | Seq

/// Represents the internal implementation of a parallel Stream of values.
type internal ParStreamImpl<'T> =    
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int 
    /// The Type of iteration source
    abstract SourceType : SourceType
    /// A flag that indicates that the ordering in the subsequent query operators will be preserved.
    abstract PreserveOrdering: bool
    /// Returns the sequential Stream
    abstract Stream : unit -> Stream<'T>
    /// Applies the given collector to the parallel Stream.
    abstract Apply<'R> : ParCollector<'T, 'R> -> 'R

/// Represents a parallel Stream of values.
type ParStream<'T> = 
    internal { Impl: ParStreamImpl<'T> }
    member internal x.Stream() = x.Impl.Stream()
    member x.DegreeOfParallelism = x.Impl.DegreeOfParallelism
    member internal x.PreserveOrdering = x.Impl.PreserveOrdering
    member internal x.SourceType = x.Impl.SourceType
    /// Applies the given collector to the parallel Stream.
    member x.Apply<'R>(collector: ParCollector<'T, 'R>) = x.Impl.Apply collector

/// Provides basic operations on Parallel Streams.
[<RequireQualifiedAccessAttribute>]
module ParStream =

    let internal ParStream x = { Impl = x }

    let private getPartitions (partitions : int) (length : int) : (int * int) [] =
        if length < 0 then raise <| new ArgumentOutOfRangeException()
        elif partitions < 1 then invalidArg "partitions" "invalid number of partitions."
        elif length = 0 then [||] else

        let chunkSize = length / partitions
        let r = length % partitions
        let ranges = new ResizeArray<int * int>()
        let mutable i = 0
        for p = 0 to partitions - 1 do
            // add a padding element for every chunk 0 <= p < r
            let j = i + chunkSize + if p < r then 1 else 0
            if j > i then
                let range = (i, j)
                ranges.Add range
            i <- j

        ranges.ToArray()

    // list partitioner:
    // returns a list of ('T list * int) elements where the list is just a tail
    // of the original list and the size of the current partition.
    // For instance, partitioning [1;2;3;4] to 2 partitions gives [|([1;2;3;4], 2) ; ([3;4], 2)|]
    // This means that no additional list allocations are made when partitioning the list.
    let private getListPartitions totalWorkers (list : 'T list) : ('T list * int) [] =
        let indices = getPartitions totalWorkers list.Length
        let ra = new ResizeArray<'T list * int> ()
        let rec aux iindex i s e rest =
            if i = s then 
                ra.Add(rest, e - s)
                aux iindex (i + 1) s e rest.Tail

            elif i = e then
                let rec nextPartition iindex =
                    if iindex = indices.Length - 1 then ()
                    else
                        let s',e' = indices.[iindex + 1]
                        if s' = e' then nextPartition (iindex + 1)
                        else
                            aux (iindex + 1) i s' e' rest

                nextPartition iindex
            else
                aux iindex (i + 1) s e (List.tail rest)

        let s,e = indices.[0] 
        aux 0 0 s e list
        ra.ToArray()


    // generator functions

    /// <summary>Wraps array as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    let ofArray (source : 'T []) : ParStream<'T> =
        ParStream
         { new ParStreamImpl<'T> with
            member self.DegreeOfParallelism = Environment.ProcessorCount
            member self.SourceType = SourceType.Array
            member self.PreserveOrdering = false
            member self.Stream () = Stream.ofArray source
            member self.Apply<'R> (collector : ParCollector<'T, 'R>) =
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
                                |> Array.map (fun (s, e) -> createTask s e (collector.Iterator()))

                    Task.WaitAll(tasks)
                collector.Result }

    /// <summary>Wraps list as a parallel stream.</summary>
    /// <param name="source">The input list.</param>
    /// <returns>The result parallel stream.</returns>
    let ofList (source : 'T list) : ParStream<'T> =
        ParStream
         { new ParStreamImpl<'T> with
            member self.DegreeOfParallelism = Environment.ProcessorCount
            member self.SourceType = SourceType.FSharpList
            member self.PreserveOrdering = false
            member self.Stream () = Stream.ofList source
            member self.Apply<'R> (collector : ParCollector<'T, 'R>) =
                if not <| List.isEmpty source then 
                    let partitions = getListPartitions collector.DegreeOfParallelism source
                    let nextRef = ref true
                    let createTask (list : 'T list) (e : int) (iter : ParIterator<'T>) =
                        iter.Cts.Token.Register(fun _ -> nextRef := false) |> ignore 
                        Task.Factory.StartNew(fun () ->
                                                let rec aux i (rest : 'T list) =
                                                    if i > 0 && !nextRef then 
                                                        iter.Func rest.Head
                                                        aux (i - 1) rest.Tail
                                                        
                                                aux e list)

                    let tasks = partitions |> Array.map (fun (tl, e) -> createTask tl e (collector.Iterator()))

                    Task.WaitAll(tasks)
                collector.Result }

    /// <summary>Wraps ResizeArray as a parallel stream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result parallel stream.</returns>
    let ofResizeArray (source : ResizeArray<'T>) : ParStream<'T> =
        ParStream
          { new ParStreamImpl<'T> with
            member self.DegreeOfParallelism = Environment.ProcessorCount
            member self.SourceType = SourceType.ResizeArray
            member self.PreserveOrdering = false
            member self.Stream () = Stream.ofResizeArray source
            member self.Apply<'R> (collector : ParCollector<'T, 'R>) =
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
                                |> Array.map (fun (s, e) -> createTask s e (collector.Iterator()))

                    Task.WaitAll(tasks)
                collector.Result }

    /// <summary>Wraps seq as a parallel stream.</summary>
    /// <param name="source">The input seq.</param>
    /// <returns>The result parallel stream.</returns>
    let ofSeq (source : seq<'T>) : ParStream<'T> =
        match source with
        | :? ('T []) as array -> ofArray array
        | :? ('T list) as list -> ofList list
        | :? ResizeArray<'T> as list -> ofResizeArray list
        | _ ->
          ParStream
            { new ParStreamImpl<'T> with
                member self.DegreeOfParallelism = Environment.ProcessorCount
                member self.SourceType = SourceType.Seq
                member self.PreserveOrdering = false
                member self.Stream () = Stream.ofSeq source
                member self.Apply<'R> (collector : ParCollector<'T, 'R>) =
                    let partitioner = Partitioner.Create(source)
                    let partitions = partitioner.GetOrderablePartitions(collector.DegreeOfParallelism).ToArray()
                    let nextRef = ref true
                    let createTask (partition : IEnumerator<KeyValuePair<int64, 'T>>) (iter : ParIterator<'T>) = 
                        iter.Cts.Token.Register(fun _ -> nextRef := false) |> ignore 
                        Task.Factory.StartNew(fun () ->
                                                while partition.MoveNext() && !nextRef do
                                                    iter.Func partition.Current.Value
                                                partition.Dispose())
                    let tasks = partitions 
                                |> Array.map (fun partition -> createTask partition  (collector.Iterator()))

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
    let withDegreeOfParallelism (degreeOfParallelism : int) (stream : ParStream<'T>) : ParStream<'T> = 
        if degreeOfParallelism < 1 then
            raise <| new ArgumentOutOfRangeException("degreeOfParallelism")
        else    
          ParStream
            { new ParStreamImpl<'T> with
                    member self.DegreeOfParallelism = degreeOfParallelism
                    member self.SourceType = stream.SourceType
                    member self.PreserveOrdering = stream.PreserveOrdering
                    member self.Stream () = stream.Stream() 
                    member self.Apply<'S> (collector : ParCollector<'T, 'S>) =
                        let collector = 
                            { new ParCollector<'T, 'S> with
                                member self.DegreeOfParallelism = degreeOfParallelism
                                member self.Iterator() = collector.Iterator()
                                member self.Result = collector.Result }
                        stream.Apply collector }

    /// <summary>Returns a parallel stream that preserves ordering.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream as ordered.</returns>
    let ordered (stream : ParStream<'T>) : ParStream<'T> = 
        ParStream
          { new ParStreamImpl<'T> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.SourceType = stream.SourceType
                member self.PreserveOrdering = true
                member self.Stream () = stream.Stream() 
                member self.Apply<'S> (collector : ParCollector<'T, 'S>) =
                    let collector = 
                        { new ParCollector<'T, 'S> with
                            member self.DegreeOfParallelism = collector.DegreeOfParallelism
                            member self.Iterator() = 
                                collector.Iterator()
                            member self.Result = collector.Result }
                    stream.Apply collector }

    /// <summary>Returns a parallel stream that is unordered.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream as unordered.</returns>
    let unordered (stream : ParStream<'T>) : ParStream<'T> = 
        if stream.PreserveOrdering then
            stream.Stream() |> Stream.toSeq |> ofSeq |> withDegreeOfParallelism stream.DegreeOfParallelism 
        else
            stream


    /// Internal entrypoints for compiled code.  
    /// Do not call these functions directly.
    module Internals = 

        /// Public permanent entrypoint used to map over a parallel stream.
        /// Do not call this function directly.
        //
        // Used to implement inlined versions of map
        let mapCont (f : ('R -> unit) -> ('T -> unit))  (stream : ParStream<'T>) : ParStream<'R> =
           ParStream
            { new ParStreamImpl<'R> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism
                member self.SourceType = stream.SourceType
                member self.PreserveOrdering = stream.PreserveOrdering
                member self.Stream () = stream.Stream() |> Stream.Internals.mapCont f 
                member self.Apply<'S> (collector : ParCollector<'R, 'S>) =
                    let collector = 
                        { new ParCollector<'T, 'S> with
                            member self.DegreeOfParallelism = collector.DegreeOfParallelism
                            member self.Iterator() = 
                                let { Func = iter } as iterator = collector.Iterator()
                                {   Func = f iter;
                                    Cts = iterator.Cts }
                            member self.Result = collector.Result }
                    stream.Apply collector }

        /// Public permanent entrypoint used to implement fold etc. via uncancellable parallel iteration.
        /// Do not call this function directly.
        //
        //  mkIterator is called each time we need an iterator
        //  collectResult is called to fetch the combined result from all iterators
        //  streamf implements the operation sequentially if ordering needs to be preserved
        let parallelIterateAndCollect (streamf: Stream<'T> -> 'R) (mkIterator: unit -> ('T -> unit)) (collectResult: unit -> 'R) (stream : ParStream<'T>) : 'R =

            let collector = 
                { new ParCollector<'T, 'R> with
                    member self.DegreeOfParallelism = stream.DegreeOfParallelism
                    member self.Iterator() = 
                        { Func = mkIterator() 
                          Cts = new CancellationTokenSource() }
                    member self.Result = collectResult() }

            if stream.PreserveOrdering then 
                match stream.SourceType with
                | SourceType.Array 
                | SourceType.FSharpList
                | SourceType.ResizeArray -> stream.Stream() |> streamf
                | SourceType.Seq -> 
                    let stream = unordered stream
                    stream.Apply collector
            else 
                stream.Apply collector


        // Internal inlined code to implement fold in terms of parallelIterateAndCollect.
        let inline internal foldInlined folder combiner state (stream : ParStream<'T>) : 'State =
            let results = new List<'State ref>()
            stream 
            |> parallelIterateAndCollect 
                (Stream.fold folder (state()))
                (fun () -> 
                    let accRef = ref (state ())
                    results.Add(accRef)
                    (fun value -> accRef := folder !accRef value))
                (fun () -> 
                    let mutable acc = state ()
                    for result in results do
                            acc <- combiner acc !result 
                    acc)

        let inline internal collectKeyValues projection (stream : ParStream<'T>) = 
            // explicit use of Tuple<ArrayCollector<'Key>, ArrayCollector<'T>> to avoid temp heap allocations of (ArrayCollector<'Key> * ArrayCollector<'T>) 
            let keyValueTuple = 
                stream |> foldInlined
                    (fun (keyValueTuple : Tuple<ArrayCollector<'Key>, ArrayCollector<'T>>) value -> 
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
                    (fun () -> new Tuple<_, _>(new ArrayCollector<'Key>(), new ArrayCollector<'T>())) 
            let keyArray, valueArray = keyValueTuple.Item1, keyValueTuple.Item2
            keyArray.ToArray(), valueArray.ToArray()

                        
        /// Public "internal" entrypoint to ensure a stream can be processed in an unordered way.
        /// Do not call this function directly.
        //
        // Public permanent entrypoint to implement inlined versions of iter, foldBy, groupBy, tryFind, tryPick etc.
        // 'streamf' is called if necessary to process elements in-order.
        let unorderedCont streamf resf (stream: ParStream<'T>) : ParStream<'U> = 
            if stream.PreserveOrdering then 
                stream.Stream() |> streamf |> Stream.toArray |> ofArray |> withDegreeOfParallelism stream.DegreeOfParallelism 
            else
                resf()

        /// Public "internal" entrypoint to iterate an unordered parallel stream and collect a result.
        /// Do not call this function directly.
        //
        // Used to implement inlined versions of iter, foldBy, groupBy, tryFind, tryPick etc.
        // 'iterf' is called with one argument before iteration.
        // 'resf' is called at the end of iteration.
        let iterCont iterf resf (stream : ParStream<'T>) = 
            let cts =  new CancellationTokenSource()
            let collector = 
                { new ParCollector<'T, 'R> with
                    member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                    member self.Iterator() = 
                        {   Func = iterf cts;
                            Cts = cts }
                    member self.Result = resf() }
        
            stream.Apply collector

        /// Public "internal" entrypoint to make one stream look like another in terms of its parallel properties.
        /// Do not call this function directly.
        //
        let looksLike (streamOrig: ParStream<_>) (stream: ParStream<'T>) = stream |> withDegreeOfParallelism streamOrig.DegreeOfParallelism
            

    // Used to indicate that we don't want a closure to be curried
    let inline internal nocurry() = Unchecked.defaultof<unit>

    /// <summary>Transforms each element of the input parallel stream.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline map (f : 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        stream |> Internals.mapCont (fun iter -> nocurry(); fun value -> iter (f value))

    /// <summary>Transforms each element of the input parallel stream. The integer index passed to the function indicates the index of element being transformed.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let mapi (f : int -> 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        ParStream
          { new ParStreamImpl<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = true
            member self.Stream () = stream.Stream() |> Stream.mapi f 
            member self.Apply<'S> (collector : ParCollector<'R, 'S>) = (unordered (ParStream self)).Impl.Apply collector }

    /// <summary>Transforms each element of the input parallel stream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input parallel stream.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let flatMap (f : 'T -> Stream<'R>) (stream : ParStream<'T>) : ParStream<'R> =
        ParStream
          { new ParStreamImpl<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream () = stream.Stream() |> Stream.flatMap f
            member self.Apply<'S> (collector : ParCollector<'R, 'S>) =
                let collector = 
                    { new ParCollector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> 
                                        let stream' = f value
                                        stream' |> Stream.Internals.iterCancelLink iterator.Cts iter)
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
        stream |> Internals.mapCont (fun iterf -> nocurry(); fun value -> if predicate value then iterf value)

    /// <summary>Applies the given function to each element of the parallel stream and returns the parallel stream comprised of the results for each element where the function returns Some with some value.</summary>
    /// <param name="chooser">A function to transform items of type 'T into options of type 'R.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline choose (chooser : 'T -> 'R option) (stream : ParStream<'T>) : ParStream<'R> =
        stream |> Internals.mapCont (fun iterf -> nocurry(); fun value -> match chooser value with | Some value' -> iterf value' | None -> ())

    /// <summary>Returns the elements of the parallel stream up to a specified count.</summary>
    /// <param name="n">The number of items to take.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result prallel stream.</returns>
    let take (n : int) (stream : ParStream<'T>) : ParStream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        ParStream
         { new ParStreamImpl<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream() = stream.Stream() |> Stream.take n
            member self.Apply<'S> (collector : ParCollector<'T, 'S>) =
                let collector = 
                    let count = ref 0
                    { new ParCollector<'T, 'S> with
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
    let skip (n : int) (stream : ParStream<'T>) : ParStream<'T> =
        ParStream
          { new ParStreamImpl<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.SourceType = stream.SourceType
            member self.PreserveOrdering = stream.PreserveOrdering
            member self.Stream() = stream.Stream() |> Stream.skip n
            member self.Apply<'S> (collector : ParCollector<'T, 'S>) =
                let collector = 
                    let count = ref 0
                    { new ParCollector<'T, 'S> with
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
    let iter (f : 'T -> unit) (stream : ParStream<'T>) : unit = 
         if stream.PreserveOrdering then  
             stream.Stream() |> Stream.iter f  
         else 
             stream |> Internals.iterCont (fun _ -> f) (fun () -> ())

    /// <summary>Applies a function to each element of the parallel stream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the parallel stream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : ParStream<'T>) : 'State =
        Internals.foldInlined folder combiner state stream

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : ParStream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        Internals.foldInlined (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    /// <summary>Returns the total number of elements of the parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The total number of elements.</returns>
    let length (stream : ParStream<'T>) : int =
        Internals.foldInlined (fun acc _  -> 1 + acc) (+) (fun () -> 0) stream

    /// <summary>Creates an array from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result array.</returns>    
    let toArray (stream : ParStream<'T>) : 'T[] =
        if stream.PreserveOrdering then
            stream.Stream() |> Stream.toArray
        else
            let arrayParCollector = 
                Internals.foldInlined
                    (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) stream 
            arrayParCollector.ToArray()

    /// <summary>Creates an ResizeArray from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result ResizeArray.</returns>    
    let toResizeArray (stream : ParStream<'T>) : ResizeArray<'T> =
        new ResizeArray<'T>(toArray stream)

    /// <summary>Creates an Seq from the given parallel stream.</summary>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result Seq.</returns>    
    let toSeq (stream : ParStream<'T>) : seq<'T> =
        match stream.SourceType, stream.PreserveOrdering with
        | SourceType.Array, false -> toArray stream :> _
        | SourceType.ResizeArray, false -> toResizeArray stream :> _
        | SourceType.FSharpList, false -> toArray stream :> _
        | SourceType.Array, true -> stream.Stream() |> Stream.toArray :> _
        | SourceType.ResizeArray, true -> stream.Stream() |> Stream.toResizeArray :> _
        | SourceType.FSharpList, true -> stream.Stream() |> Stream.toArray :> _
        | SourceType.Seq, _ -> stream.Stream() |> Stream.toSeq
        

    /// <summary>Locates the maximum element of the parallel stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="source">The input parallel stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline maxBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : ParStream<'T>) : 'T =
        let result =
            source |> Internals.foldInlined
              (fun state t -> 
                  let key = projection t 
                  match state with 
                  | None -> Some (ref t, ref key)
                  | Some (refValue, refKey) when !refKey < key -> 
                      refValue := t
                      refKey := key
                      state
                  | _ -> state) 
              (fun left right -> 
                  match left, right with
                  | Some (_, key), Some (_, key') ->
                      if !key' > !key then right else left
                  | None, _ -> right
                  | _, None -> left) 
              (fun () -> None) 

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (refValue,_) -> !refValue

    /// <summary>Locates the minimum element of the parallel stream by given key.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="source">The input parallel stream.</param>
    /// <returns>The maximum item.</returns>  
    let inline minBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (source : ParStream<'T>) : 'T =
        let result = 
            source |> Internals.foldInlined
              (fun state t ->
                  let key = projection t 
                  match state with 
                  | None -> Some (ref t, ref key)
                  | Some (refValue, refKey) when !refKey > key -> 
                      refValue := t
                      refKey := key
                      state 
                  | _ -> state) 
              (fun left right -> 
                  match left, right with
                  | Some (_, key), Some (_, key') ->
                      if !key' > !key then left else right
                  | None, _ -> right
                  | _, None -> left) 
              (fun () -> None) 

        match result with
        | None -> invalidArg "source" "The input sequence was empty."
        | Some (refValue,_) -> !refValue


    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>    
    let inline sortBy<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'T>  =
        let keyArray, valueArray = Internals.collectKeyValues projection stream
        Sort.parallelSort stream.DegreeOfParallelism false keyArray valueArray
        valueArray |> ofArray |> ordered |> Internals.looksLike stream

    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered by keys in descending order.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>    
    let inline sortByDescending<'T, 'Key when 'Key : comparison> (projection : 'T -> 'Key) (stream : ParStream<'T>) : ParStream<'T>  =
        let keyArray, valueArray = Internals.collectKeyValues projection stream
        Sort.parallelSort stream.DegreeOfParallelism true keyArray valueArray
        valueArray |> ofArray |> ordered |> Internals.looksLike stream

    /// <summary>Applies a key-generating function to each element of the input parallel stream and yields a parallel stream ordered using the given comparer for the keys.</summary>
    /// <param name="projection">A function to transform items of the input parallel stream into comparable keys.</param>
    /// <param name="flow">The input parallel stream.</param>
    /// <returns>The result parallel stream.</returns>
    let inline sortByUsing<'T, 'Key> (projection : 'T -> 'Key) (comparer : IComparer<'Key>) (stream : ParStream<'T>) : ParStream<'T> =
        let keyArray, valueArray = Internals.collectKeyValues projection stream
        Sort.parallelSortWithComparer stream.DegreeOfParallelism comparer keyArray valueArray
        valueArray |> ofArray |> ordered |> Internals.looksLike stream

    /// <summary>Applies a key-generating function to each element of a ParStream and return a ParStream yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input ParStream to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the ParStream.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input ParStream.</param>
    /// <returns>The final result.</returns>
    let inline foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (state : unit -> 'State) (stream : ParStream<'T>) : ParStream<'Key * 'State> =

        // First make sure we can do an unordered traversal.  If we can't just use Stream based ordered traversal
        stream |> Internals.unorderedCont 
           (fun stream -> Stream.foldBy projection folder state stream) 
           (fun () ->
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            stream |> Internals.iterCont 
                (fun cts -> nocurry(); fun value -> 
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
                        lock grouping (fun () -> acc := folder !acc value))
                 (fun () -> 
                    dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, !keyValue.Value)) |> Internals.looksLike stream))
                

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

        // First make sure we can do an unordered traversal.  If we can't just use Stream based ordered traversal
        stream |> Internals.unorderedCont 
           (fun stream -> Stream.groupBy projection stream) 
           (fun () ->
            let dict = new ConcurrentDictionary<'Key, List<'T>>()
            stream |> Internals.iterCont 
                (fun cts -> nocurry(); fun value -> 
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
                        lock grouping (fun () -> list.Add(value)))
                 (fun () -> 
                    let stream' = dict |> ofSeq |> map (fun keyValue -> (keyValue.Key, keyValue.Value :> seq<'T>))   
                    stream' |> Internals.looksLike stream))
        

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input parallel stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (stream : ParStream<'T>) : 'T option = 
        let resultRef = ref Unchecked.defaultof<'T option>
        stream 
        |> unordered
        |> Internals.iterCont 
            (fun cts -> nocurry(); fun value -> if predicate value then resultRef := Some value; cts.Cancel())
            (fun _ -> !resultRef)

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
        stream 
        |> unordered
        |> Internals.iterCont 
            (fun cts -> nocurry(); fun value -> match chooser value with Some value' -> resultRef := Some value'; cts.Cancel() | None -> ())
            (fun _ -> !resultRef)


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
    let tryHead (stream : ParStream<'T>) : 'T option =
        let r = stream |> ordered |> take 1 |> toArray
        if r.Length = 0 then None
        else Some r.[0]


    /// <summary>
    ///     Returs the first element of the stream.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>The first element of the stream.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the stream has no elements.</exception>
    let head (stream : ParStream<'T>) : 'T =
        match tryHead stream with
        | Some value -> value
        | None -> invalidArg "stream" "The stream was empty."

    /// <summary>
    ///     Returs true if the stream is empty and false otherwise.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <returns>true if the input stream is empty, false otherwise</returns>
    let isEmpty (stream : ParStream<'T>) : bool =
        stream |> exists (fun _ -> true) |> not
