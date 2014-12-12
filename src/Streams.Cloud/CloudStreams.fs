namespace Nessos.Streams.Cloud
open System
open System.Collections.Generic
open System.Linq
open Nessos.MBrace
open Nessos.Streams
open Nessos.Streams.Internals

/// Represents a distributed Stream of values.
type CloudStream<'T> = 
    /// Applies the given collector to the CloudStream.
    abstract Apply<'S, 'R> : (unit -> Collector<'T, 'S>) -> ('S -> Cloud<'R>) -> ('R -> 'R -> 'R) -> Cloud<'R>

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides basic operations on CloudStreams.
module CloudStream =

    /// Maximum combined stream length used in ofCloudFiles.
    let private maxCloudFileCombinedLength = 1024L * 1024L * 1024L

    /// <summary>Wraps array as a CloudStream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudStream.</returns>
    let ofArray (source : 'T []) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud {
                    let! workerCount = Cloud.GetWorkerCount()
                    let createTask array (collector : Collector<'T, 'S>) = 
                        cloud {
                            let parStream = ParStream.ofArray array 
                            do parStream.Apply collector
                            return! projection collector.Result
                        }
                    if not (source.Length = 0) then 
                        let partitions = Partitions.ofLongRange workerCount (int64 source.Length)
                        let! results = partitions |> Array.map (fun (s, e) -> createTask [| for i in s..(e - 1L) do yield source.[int i] |] (collectorf ())) |> Cloud.Parallel
                        return Array.reduce combiner results
                    else
                        return! projection (collectorf ()).Result
                } }

    /// <summary>
    /// Constructs a CloudStream from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to an object.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    let ofCloudFiles (reader : System.IO.Stream -> Async<'T>) (sources : seq<ICloudFile>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud { 
                    if Seq.isEmpty sources then 
                        return! projection (collectorf ()).Result
                    else
                        let! workerCount = Cloud.GetWorkerCount()

                        let createTask (files : ICloudFile []) (collectorf : unit -> Collector<'T, 'S>) : Cloud<'R> = 
                            cloud {
                                let rec partitionByLength (files : ICloudFile []) index (currLength : int64) (currAcc : ICloudFile list) (acc : ICloudFile list list)=
                                    async {
                                        if index >= files.Length then return (currAcc :: acc) |> List.filter (not << List.isEmpty)
                                        else
                                            use! stream = files.[index].Read()
                                            if stream.Length >= maxCloudFileCombinedLength then
                                                return! partitionByLength files (index + 1) stream.Length [files.[index]] (currAcc :: acc)
                                            elif stream.Length + currLength >= maxCloudFileCombinedLength then
                                                return! partitionByLength files index 0L [] (currAcc :: acc)
                                            else
                                                return! partitionByLength files (index + 1) (currLength + stream.Length) (files.[index] :: currAcc) acc
                                    }
                                let! partitions = partitionByLength files 0 0L [] [] |> Cloud.OfAsync

                                let result = new ResizeArray<'R>(partitions.Length)
                                for fs in partitions do
                                    let collector = collectorf()
                                    let parStream = 
                                        fs
                                        |> ParStream.ofSeq 
                                        |> ParStream.map (fun file -> async { let! s = file.Read() in return! reader s })
                                        |> ParStream.map Async.RunSynchronously
                                    parStream.Apply collector
                                    let! partial = projection collector.Result
                                    result.Add(partial)
                                if result.Count = 0 then
                                    return! projection (collectorf ()).Result
                                else
                                    return Array.reduce combiner (result.ToArray())
                            }

                        let partitions = sources |> Seq.toArray |> Partitions.ofArray workerCount
                        let! results = partitions |> Array.map (fun cfiles -> createTask cfiles collectorf) |> Cloud.Parallel
                        return Array.reduce combiner results
                } }

    /// <summary>
    /// Constructs a CloudStream from a CloudArray.
    /// </summary>
    /// <param name="source">The input CloudArray.</param>
    let ofCloudArray (source : ICloudArray<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud {
                    let! workerCount = Cloud.GetWorkerCount()

                    let createTask (partitionId : int) (collector : Collector<'T, 'S>) = 
                        cloud {
                            let array = source.GetPartition(partitionId) 
                            let parStream = ParStream.ofArray array 
                            do parStream.Apply collector
                            return! projection collector.Result
                        }

                    let createTaskCached (cached : CachedCloudArray<'T>) (taskId : string) (collectorf : unit -> Collector<'T, 'S>) = 
                        cloud { 
                            let partitions = CloudArrayCache.Get(cached, taskId) |> Seq.toArray
                            let completed = new ResizeArray<int * 'R>()
                            for pid in partitions do
                                let array = CloudArrayCache.GetPartition(cached, pid)
                                let parStream = ParStream.ofArray array
                                let collector = collectorf()
                                parStream.Apply collector
                                let! partial = projection collector.Result
                                completed.Add(pid, partial)
                            return completed 
                        }
                    
                    if source.Length = 0L then
                        return! projection (collectorf ()).Result;
                    else
                        let partitions = [|0..source.Partitions-1|]
                        match source with
                        | :? CachedCloudArray<'T> as cached -> 
                            // round 1
                            let taskId = Guid.NewGuid().ToString() //Cloud.GetTaskId()
                            let! partial = 
                                Array.init partitions.Length (fun _ -> createTaskCached cached taskId collectorf) 
                                |> Cloud.Parallel
                            let results1 = Seq.concat partial 
                                           |> Seq.toArray 
                            let completedPartitions = 
                                results1
                                |> Seq.map (fun (p, _) -> p)
                                |> Set.ofSeq
                            let allPartitions = partitions |> Set.ofSeq
                            // round 2
                            let restPartitions = allPartitions - completedPartitions
                            
                            if Seq.isEmpty restPartitions then
                                let final = results1 
                                            |> Seq.sortBy (fun (p,_) -> p)
                                            |> Seq.map (fun (_,r) -> r) 
                                            |> Seq.toArray
                                return Array.reduce combiner final
                            else
                                let! results2 = restPartitions 
                                                |> Set.toArray 
                                                |> Array.map (fun pid -> cloud { let! r = createTask pid (collectorf ()) in return pid,r }) 
                                                |> Cloud.Parallel
                                let final = Seq.append results1 results2
                                            |> Seq.sortBy (fun (p,_) -> p)
                                            |> Seq.map (fun (_,r) -> r)
                                            |> Seq.toArray
                                return Array.reduce combiner final
                        | source -> 
                            let! results = partitions |> Array.map (fun partitionId -> createTask partitionId (collectorf ())) |> Cloud.Parallel
                            return Array.reduce combiner results
                            
                } }

    /// <summary>
    /// Returns a cached version of the given CloudArray.
    /// </summary>
    /// <param name="source">The input CloudArray.</param>
    let cache (source : ICloudArray<'T>) : Cloud<ICloudArray<'T>> = 
        cloud {
            let! workerCount = Cloud.GetWorkerCount()
            let createTask (pid : int) (cached : CachedCloudArray<'T>) = 
                cloud {
                    let slice = (cached :> ICloudArray<'T>).GetPartition(pid)
                    CloudArrayCache.Add(cached, pid, slice)
                }
            let taskId = Guid.NewGuid().ToString()
            let cached = new CachedCloudArray<'T>(source, taskId)
            if source.Length > 0L then
                let partitions = [|0..source.Partitions-1|]
                do! partitions 
                    |> Array.map (fun pid -> createTask pid cached) 
                    |> Cloud.Parallel
                    |> Cloud.Ignore
            return cached :> _
        }

    // intermediate functions

    /// <summary>Transforms each element of the input CloudStream.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline map (f : 'T -> 'R) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S, 'Result> (collectorf : unit -> Collector<'R, 'S>) (projection : 'S -> Cloud<'Result>) combiner =
                let collectorf' () = 
                    let collector = collectorf ()
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   OrderIndex = iterator.OrderIndex; Index = iterator.Index; 
                                Complete = iterator.Complete;
                                Func = (fun value -> iter (f value)) }
                        member self.Result = collector.Result  }
                stream.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudStream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline flatMap (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S, 'Result> (collectorf : unit -> Collector<'R, 'S>) (projection : 'S -> Cloud<'Result>) combiner =
                let collectorf' () = 
                    let collector = collectorf ()
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   OrderIndex = iterator.OrderIndex; Index = iterator.Index; 
                                Complete = iterator.Complete;
                                Func = 
                                    (fun value -> 
                                        let (Stream streamf) = f value
                                        let { Bulk = bulk; Iterator = _ } = streamf (fun () -> ()) iter in bulk (); true) }
                        member self.Result = collector.Result  }
                stream.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudStream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline collect (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        flatMap f stream

    /// <summary>Filters the elements of the input CloudStream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline filter (predicate : 'T -> bool) (stream : CloudStream<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) combiner =
                let collectorf' () = 
                    let collector = collectorf ()
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   OrderIndex = iterator.OrderIndex; Index = iterator.Index; 
                                Complete = iterator.Complete
                                Func = (fun value -> if predicate value then iter value else true) }
                        member self.Result = collector.Result }
                stream.Apply collectorf' projection combiner }


    // terminal functions

    /// <summary>Applies a function to each element of the CloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : CloudStream<'T>) : Cloud<'State> =
            let collectorf () =  
                let results = new List<'State ref>()
                { new Collector<'T, 'State> with
                    member self.Iterator() = 
                        let accRef = ref <| state ()
                        results.Add(accRef)
                        {   OrderIndex = ref -1; Index = ref -1;
                            Complete = (fun () -> ());
                            Func = (fun value -> accRef := folder !accRef value; true) }
                    member self.Result = 
                        let mutable acc = state ()
                        for result in results do
                             acc <- combiner acc !result 
                        acc }
            stream.Apply collectorf (fun x -> cloud { return x }) combiner

    /// <summary>Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input CloudStream to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The final result.</returns>
    let inline foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (combiner : 'State -> 'State -> 'State) 
                      (state : unit -> 'State) (stream : CloudStream<'T>) : CloudStream<'Key * 'State> =
            let collectorf () =  
                let results = new List<Dictionary<'Key, 'State ref>>()
                { new Collector<'T,  Dictionary<'Key, 'State ref>> with
                    member self.Iterator() = 
                        let dict = new Dictionary<'Key, 'State ref>()
                        results.Add(dict)
                        {   OrderIndex = ref -1; Index = ref -1; 
                            Complete = (fun () -> ());
                            Func =
                                (fun value -> 
                                        let key = projection value
                                        let mutable stateRef = Unchecked.defaultof<'State ref>
                                        if dict.TryGetValue(key, &stateRef) then
                                            stateRef := folder !stateRef value
                                        else
                                            stateRef <- ref <| state ()
                                            stateRef := folder !stateRef value
                                            dict.Add(key, stateRef)
                                        true) }
                    member self.Result = 
                        let dict = new Dictionary<'Key, 'State ref>()
                        for result in results do
                            for keyValue in result do
                                let mutable stateRef = Unchecked.defaultof<'State ref>
                                if dict.TryGetValue(keyValue.Key, &stateRef) then
                                    stateRef := combiner !stateRef !keyValue.Value
                                else
                                    stateRef <- ref <| state ()
                                    stateRef := combiner !stateRef !keyValue.Value
                                    dict.Add(keyValue.Key, stateRef)
                        dict  }
            let foldByComp = 
                cloud {
                    let combiner' (left : ICloudArray<KeyValuePair<'Key, 'State ref>>) (right : ICloudArray<KeyValuePair<'Key, 'State ref>>) = 
                        left.Append(right)
                    let! keyValueArray = stream.Apply collectorf (fun dict -> cloud { let! processId = Cloud.GetProcessId() in return! CloudArray.New(sprintf "process%d" processId, dict) }) combiner'
                    let dict = 
                        let dict = new Dictionary<'Key, 'State ref>()
                        for keyValue in keyValueArray do
                            let mutable stateRef = Unchecked.defaultof<'State ref>
                            if dict.TryGetValue(keyValue.Key, &stateRef) then
                                stateRef := combiner !stateRef !keyValue.Value
                            else
                                stateRef <- ref <| state ()
                                stateRef := combiner !stateRef !keyValue.Value
                                dict.Add(keyValue.Key, stateRef)
                        dict
                    let keyValues = dict |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) |> Seq.toArray 
                    return keyValues 
                }
            { new CloudStream<'Key * 'State> with
                member self.Apply<'S, 'R> (collectorf : unit -> Collector<'Key * 'State, 'S>) (projection : 'S -> Cloud<'R>) combiner =
                    cloud {
                        let! result = foldByComp
                        return! (ofArray result).Apply collectorf projection combiner
                    }  }
    /// <summary>
    /// Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudStream to keys.</param>
    /// <param name="stream">The input CloudStream.</param>
    let inline countBy (projection : 'T -> 'Key) (stream : CloudStream<'T>) : CloudStream<'Key * int64> =
        foldBy projection (fun state _ -> state + 1L) (+) (fun () -> 0L) stream

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : CloudStream< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    /// <summary>Returns the total number of elements of the CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (stream : CloudStream<'T>) : Cloud<int64> =
        fold (fun acc _  -> 1L + acc) (+) (fun () -> 0L) stream

    /// <summary>Creates an array from the given CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (stream : CloudStream<'T>) : Cloud<'T[]> =
        cloud {
            let! arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) stream 
            return arrayCollector.ToArray()
        }

    /// <summary>Creates a CloudArray from the given CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudArray.</returns>    
    let inline toCloudArray (stream : CloudStream<'T>) : Cloud<ICloudArray<'T>> =
        let collectorf () =  
            let results = new List<List<'T>>()
            { new Collector<'T, 'T []> with
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    {   OrderIndex = ref -1; Index = ref -1; 
                        Complete = (fun () -> ());
                        Func = (fun value -> list.Add(value); true) }
                member self.Result = 
                    let count = results |> Seq.sumBy (fun list -> list.Count)
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for list in results do
                        for i = 0 to list.Count - 1 do
                            let value = list.[i]
                            counter <- counter + 1
                            values.[counter] <- value
                    values }
        stream.Apply collectorf (fun array -> cloud { 
                                                let! processId = Cloud.GetProcessId() 
                                                return! CloudArray.New(sprintf "process%d" processId, array) }) 
                                (fun left right -> left.Append(right))

    /// <summary>Applies a key-generating function to each element of the input CloudStream and yields the CloudStream of the given length, ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudStream into comparable keys.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudStream.</returns>  
    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (stream : CloudStream<'T>) : CloudStream<'T> = 
        let collectorf () =  
            let results = new List<List<'T>>()
            { new Collector<'T, List<'Key[] * 'T []>> with
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    {   OrderIndex = ref -1; Index = ref -1; 
                        Complete = (fun () -> ());
                        Func = (fun value -> list.Add(value); true) }
                member self.Result = 
                    let count = results |> Seq.sumBy (fun list -> list.Count)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for list in results do
                        for i = 0 to list.Count - 1 do
                            let value = list.[i]
                            counter <- counter + 1
                            keys.[counter] <- projection value
                            values.[counter] <- value
                    Sort.parallelSort keys values
                    new List<_>(Seq.singleton
                                    (keys.Take(takeCount).ToArray(), 
                                     values.Take(takeCount).ToArray())) }
        let sortByComp = 
            cloud {
                let! results = stream.Apply collectorf (fun x -> cloud { return x }) (fun left right -> left.AddRange(right); left)
                let result = 
                    let count = results |> Seq.sumBy (fun (keys, _) -> keys.Length)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for (keys', values') in results do
                        for i = 0 to keys'.Length - 1 do
                            counter <- counter + 1
                            keys.[counter] <- keys'.[i]
                            values.[counter] <- values'.[i]
                    Sort.parallelSort keys values    
                    values.Take(takeCount).ToArray()
                return result
            }
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (ofArray result).Apply collectorf projection combiner
                }  }


