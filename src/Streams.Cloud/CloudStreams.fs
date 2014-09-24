namespace Nessos.Streams.Cloud
open System
open System.Collections.Generic
open System.Linq
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Nessos.MBrace
open Nessos.Streams.Core


type CloudStream<'T> = 
    abstract Apply<'R> : (unit -> Collector<'T, 'R>) -> ('R -> 'R -> 'R) -> Cloud<'R>

module CloudStream =

    let internal getPartitions (totalWorkers : int) (s : int64) (e : int64) = 
        let toSeq (enum : IEnumerator<_>)= 
            seq {
                while enum.MoveNext() do
                    yield enum.Current
            }
        let partitioner = Partitioner.Create(s, e)
        let partitions = partitioner.GetPartitions(totalWorkers) |> Seq.collect toSeq |> Seq.toArray 
        partitions

    // generator functions
    let ofArray (source : 'T []) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S> (collectorf : unit -> Collector<'T, 'S>) combiner =
                cloud {
                    let! workerCount = Cloud.GetWorkerCount()
                    let createTask array (collector : Collector<'T, 'S>) = 
                        cloud {
                            let parStream = ParStream.ofArray array 
                            do parStream.Apply collector
                            return  collector.Result
                        }
                    if not (source.Length = 0) then 
                        let partitions = getPartitions workerCount 0L (int64 source.Length)
                        let! results = partitions |> Array.map (fun (s, e) -> createTask [| for i in s..(e - 1L) do yield source.[int i] |] (collectorf ())) |> Cloud.Parallel
                        return Array.reduce combiner results
                    else
                        return (collectorf ()).Result;
                } }


    let ofCloudArray (source : ICloudArray<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S> (collectorf : unit -> Collector<'T, 'S>) combiner =
                cloud {
                    let! workerCount = Cloud.GetWorkerCount()
                    // TODO: int64 partition sizes
                    let createTask (s : int64) (e : int64) (collector : Collector<'T, 'S>) = 
                        cloud {
                            let array = source.Range(s, int (e - s))
                            let parStream = ParStream.ofArray array 
                            do parStream.Apply collector
                            return  collector.Result
                        }
                    if not (source.Length = 0L) then 
                        let partitions = getPartitions workerCount 0L source.Length
                        let! results = partitions |> Array.map (fun (s, e) -> createTask s e (collectorf ())) |> Cloud.Parallel
                        return Array.reduce combiner results
                    else
                        return (collectorf ()).Result;
                } }


    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S> (collectorf : unit -> Collector<'R, 'S>) combiner =
                let collector = collectorf ()
                let collectorf' () = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> iter (f value))
                        member self.Result = collector.Result  }
                stream.Apply collectorf' combiner }


    let inline flatMap (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S> (collectorf : unit -> Collector<'R, 'S>) combiner =
                let collector = collectorf ()
                let collectorf' () = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> 
                                let (Stream streamf) = f value
                                streamf iter; true)
                        member self.Result = collector.Result  }
                stream.Apply collectorf' combiner }

    let inline collect (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : CloudStream<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S> (collectorf : unit -> Collector<'T, 'S>) combiner =
                let collector = collectorf ()
                let collectorf' () = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> if predicate value then iter value else true)
                        member self.Result = collector.Result }
                stream.Apply collectorf' combiner }


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
            stream.Apply collectorf combiner

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
                    let! dict = stream.Apply collectorf combiner'
                    return dict |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) |> Seq.toArray
                }
            { new CloudStream<'Key * 'State> with
                member self.Apply<'S> (collectorf : unit -> Collector<'Key * 'State, 'S>) combiner =
                    cloud {
                        let! result = foldByComp
                        return! (ofArray result).Apply collectorf combiner
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
        
        raise <| new NotImplementedException()

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
                let! results = stream.Apply collectorf (fun left right -> left.AddRange(right); left)
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
            member self.Apply<'S> (collectorf : unit -> Collector<'T, 'S>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (ofArray result).Apply collectorf combiner
                }  }


