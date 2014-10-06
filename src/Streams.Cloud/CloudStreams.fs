namespace Nessos.Streams.Cloud
open System
open System.Collections.Generic
open System.Linq
open Nessos.MBrace
open Nessos.Streams.Core


type CloudStream<'T> = 
    abstract Apply<'S, 'R> : (unit -> Collector<'T, 'S>) -> ('S -> Cloud<'R>) -> ('R -> 'R -> 'R) -> Cloud<'R>

[<RequireQualifiedAccess>]
module CloudStream =

    // generator functions
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
                        let partitions = Partitions.ofRange workerCount 0L (int64 source.Length)
                        let! results = partitions |> Array.map (fun (s, e) -> createTask [| for i in s..(e - 1L) do yield source.[int i] |] (collectorf ())) |> Cloud.Parallel
                        return Array.reduce combiner results
                    else
                        return! projection (collectorf ()).Result
                } }

    let ofCloudArray (source : ICloudArray<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud {
                    let! workerCount = Cloud.GetWorkerCount()

                    let createTask (s : int64) (e : int64) (collector : Collector<'T, 'S>) = 
                        cloud {
                            let array = source.Range(s, int (e - s)) // TODO: allow larger ranges
                            let parStream = ParStream.ofArray array 
                            do parStream.Apply collector
                            return! projection collector.Result
                        }

                    let createTaskCached (cached : CachedCloudArray<'T>) (taskId : string) (collectorf : unit -> Collector<'T, 'S>) = 
                        cloud { 
                            let ranges = CloudArrayCache.Get(cached, taskId) |> Seq.toArray
                            let completed = new ResizeArray<int64 * int64 * 'R>()
                            for start, count in ranges do
                                let array = CloudArrayCache.GetRange(cached, start, count)
                                let parStream = ParStream.ofArray array
                                let collector = collectorf()
                                parStream.Apply collector
                                let! partial = projection collector.Result
                                completed.Add(start, start + int64 count, partial)
                            return completed 
                        }
                    
                    if source.Length = 0L then
                        return! projection (collectorf ()).Result;
                    else
                        let partitions = Partitions.ofRange workerCount 0L source.Length
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
                                |> Seq.map (fun (s, e, _) -> s, e)
                                |> Set.ofSeq
                            let allPartitions = partitions |> Set.ofSeq
                            // round 2
                            let restPartitions = allPartitions - completedPartitions
                            
                            if Seq.isEmpty restPartitions then
                                let final = results1 
                                            |> Seq.sortBy (fun (s,_,_) -> s)
                                            |> Seq.map (fun (_,_,r) -> r) 
                                            |> Seq.toArray
                                return Array.reduce combiner final
                            else
                                let! results2 = restPartitions 
                                                |> Set.toArray 
                                                |> Array.map (fun (s, e) -> cloud { let! r = createTask s e (collectorf ()) in return s,e,r }) 
                                                |> Cloud.Parallel
                                let final = Seq.append results1 results2
                                            |> Seq.sortBy (fun (s,_,_) -> s)
                                            |> Seq.map (fun (_,_,r) -> r)
                                            |> Seq.toArray
                                return Array.reduce combiner final
                        | source -> 
                            let! results = partitions |> Array.map (fun (s, e) -> createTask s e (collectorf ())) |> Cloud.Parallel
                            return Array.reduce combiner results
                            
                } }

    let cache (source : ICloudArray<'T>) : Cloud<ICloudArray<'T>> = 
        cloud {
            let! workerCount = Cloud.GetWorkerCount()
            let createTask (s : int64) (e : int64) (cached : CachedCloudArray<'T>) = 
                // TODO: Allow larger partition ranges
                if e - s > int64 Int32.MaxValue then 
                    failwith "Partition range exceeds max value"
                cloud {
                    let count = int (e - s)
                    let slice = (cached :> ICloudArray<'T>).Range(s, count)
                    CloudArrayCache.Add(cached, s, count, slice)
                }
            let taskId = Guid.NewGuid().ToString()
            let cached = new CachedCloudArray<'T>(source, taskId)
            if source.Length > 0L then
                let partitions = Partitions.ofRange workerCount 0L source.Length
                do! partitions 
                    |> Array.map (fun (s, e) -> createTask s e cached) 
                    |> Cloud.Parallel
                    |> Cloud.Ignore
            return cached :> _
        }

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S, 'Result> (collectorf : unit -> Collector<'R, 'S>) (projection : 'S -> Cloud<'Result>) combiner =
                let collectorf' () = 
                    let collector = collectorf ()
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> iter (f value))
                        member self.Result = collector.Result  }
                stream.Apply collectorf' projection combiner }


    let inline flatMap (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S, 'Result> (collectorf : unit -> Collector<'R, 'S>) (projection : 'S -> Cloud<'Result>) combiner =
                let collectorf' () = 
                    let collector = collectorf ()
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> 
                                let (Stream streamf) = f value
                                streamf iter; true)
                        member self.Result = collector.Result  }
                stream.Apply collectorf' projection combiner }

    let inline collect (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : CloudStream<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S, 'R> (collectorf : unit -> Collector<'T, 'S>) (projection : 'S -> Cloud<'R>) combiner =
                let collectorf' () = 
                    let collector = collectorf ()
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> if predicate value then iter value else true)
                        member self.Result = collector.Result }
                stream.Apply collectorf' projection combiner }


    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : CloudStream<'T>) : Cloud<'State> =
            let collectorf () =  
                let results = new List<'State ref>()
                { new Collector<'T, 'State> with
                    member self.Iterator() = 
                        let accRef = ref <| state ()
                        results.Add(accRef)
                        (fun value -> accRef := folder !accRef value; true)
                    member self.Result = 
                        let mutable acc = state ()
                        for result in results do
                             acc <- combiner acc !result 
                        acc }
            stream.Apply collectorf (fun x -> cloud { return x }) combiner

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
                        (fun value -> 
                                let key = projection value
                                let mutable stateRef = Unchecked.defaultof<'State ref>
                                if dict.TryGetValue(key, &stateRef) then
                                    stateRef := folder !stateRef value
                                else
                                    stateRef <- ref <| state ()
                                    stateRef := folder !stateRef value
                                    dict.Add(key, stateRef)
                                true)
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
                    let combiner' (left : Dictionary<'Key, 'State ref>) (right : Dictionary<'Key, 'State ref>) = 
                        for keyValue in right do
                            let mutable stateRef = Unchecked.defaultof<'State ref>
                            if left.TryGetValue(keyValue.Key, &stateRef) then
                                stateRef := combiner !stateRef !keyValue.Value
                            else
                                stateRef <- ref <| state ()
                                stateRef := combiner !stateRef !keyValue.Value
                                left.Add(keyValue.Key, stateRef)
                        left
                    let! dict = stream.Apply collectorf (fun x -> cloud { return x }) combiner'
                    return dict |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) |> Seq.toArray
                }
            { new CloudStream<'Key * 'State> with
                member self.Apply<'S, 'R> (collectorf : unit -> Collector<'Key * 'State, 'S>) (projection : 'S -> Cloud<'R>) combiner =
                    cloud {
                        let! result = foldByComp
                        return! (ofArray result).Apply collectorf projection combiner
                    }  }

    let inline countBy (projection : 'T -> 'Key) (stream : CloudStream<'T>) : CloudStream<'Key * int> =
        foldBy projection (fun state _ -> state + 1) (+) (fun () -> 0) stream

    let inline sum (stream : CloudStream< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    let inline length (stream : CloudStream<'T>) : Cloud<int64> =
        fold (fun acc _  -> 1L + acc) (+) (fun () -> 0L) stream

    let inline toArray (stream : CloudStream<'T>) : Cloud<'T[]> =
        cloud {
            let! arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) stream 
            return arrayCollector.ToArray()
        }

    let inline toCloudArray (stream : CloudStream<'T>) : Cloud<ICloudArray<'T>> =
        let collectorf () =  
            let results = new List<List<'T>>()
            { new Collector<'T, 'T []> with
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    (fun value -> list.Add(value); true)
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

    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (stream : CloudStream<'T>) : CloudStream<'T> = 
        let collectorf () =  
            let results = new List<List<'T>>()
            { new Collector<'T, List<'Key[] * 'T []>> with
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    (fun value -> list.Add(value); true)
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


