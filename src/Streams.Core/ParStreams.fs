namespace Nessos.Streams.Core
open System
open System.Collections.Generic
open System.Linq
open System.Collections.Concurrent
open System.Threading.Tasks


type Collector<'T, 'R> = 
    abstract Iterator : unit -> ('T -> bool)
    abstract Result : 'R

type ParStream<'T> = 
    abstract Apply<'R> : Collector<'T, 'R> -> unit

module ParStream =

    let internal  totalWorkers = int (2.0 ** float (int (Math.Log(float Environment.ProcessorCount, 2.0))))

    let internal getPartitions (s : int, e : int) = 
            let toSeq (enum : IEnumerator<_>)= 
                seq {
                    while enum.MoveNext() do
                        yield enum.Current
                }
            let partitioner = Partitioner.Create(s, e)
            let partitions = partitioner.GetPartitions(totalWorkers) |> Seq.collect toSeq |> Seq.toArray 
            partitions

    // generator functions
    let ofArray (source : 'T []) : ParStream<'T> =
        { new ParStream<'T> with
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                if not (source.Length = 0) then 
                    let partitions = getPartitions(0, source.Length)
                    let createTask s e iter = 
                        Task.Factory.StartNew(fun () ->
                                                let mutable i = s
                                                let mutable next = true
                                                while i < e && next do
                                                    next <- iter source.[i]
                                                    i <- i + 1 
                                                ())
                    let tasks = partitions |> Array.map (fun (s, e) -> 
                                                            let iter = collector.Iterator()
                                                            createTask s e iter)

                    Task.WaitAll(tasks) }

    let ofResizeArray (source : ResizeArray<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                if not (source.Count = 0) then 
                    let partitions = getPartitions(0, source.Count)
                    let createTask s e iter = 
                        Task.Factory.StartNew(fun () ->
                                                let mutable i = s
                                                let mutable next = true
                                                while i < e && next do
                                                    next <- iter source.[i]
                                                    i <- i + 1 
                                                ())
                    let tasks = partitions |> Array.map (fun (s, e) -> 
                                                            let iter = collector.Iterator()
                                                            createTask s e iter)

                    Task.WaitAll(tasks) }

    let ofSeq (source : seq<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.Apply<'R> (collector : Collector<'T, 'R>) =
                
                let partitioner = Partitioner.Create(source)
                let partitions = partitioner.GetPartitions(totalWorkers).ToArray()
                let createTask (partition : IEnumerator<'T>) iter = 
                    Task.Factory.StartNew(fun () ->
                                            let mutable next = true
                                            while partition.MoveNext() && next do
                                                next <- iter partition.Current
                                            ())
                let tasks = partitions |> Array.map (fun partition -> 
                                                        let iter = collector.Iterator()
                                                        createTask partition iter)

                Task.WaitAll(tasks) }


    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> iter (f value))
                        member self.Result = collector.Result  }
                stream.Apply collector }

    let inline flatMap (f : 'T -> Stream<'R>) (stream : ParStream<'T>) : ParStream<'R> =
        { new ParStream<'R> with
            member self.Apply<'S> (collector : Collector<'R, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> 
                                let (Stream streamf) = f value
                                streamf iter; true)
                        member self.Result = collector.Result  }
                stream.Apply collector }

    let inline collect (f : 'T -> Stream<'R>) (stream : ParStream<'T>) : ParStream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : ParStream<'T>) : ParStream<'T> =
        { new ParStream<'T> with
            member self.Apply<'S> (collector : Collector<'T, 'S>) =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> if predicate value then iter value else true)
                        member self.Result = collector.Result }
                stream.Apply collector }

    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : ParStream<'T>) : 'State =

        let results = new List<'State ref>()
        let collector = 
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
        stream.Apply collector
        collector.Result

    let inline sum (stream : ParStream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    let inline length (stream : ParStream<'T>) : int =
        fold (fun acc _  -> 1 + acc) (+) (fun () -> 0) stream

    let inline toArray (stream : ParStream<'T>) : 'T[] =
        let arrayCollector = 
            fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                (fun left right -> left.AddRange(right); left) 
                (fun () -> new ArrayCollector<'T>()) stream 
        arrayCollector.ToArray()

    let inline toResizeArray (stream : ParStream<'T>) : ResizeArray<'T> =
        new ResizeArray<'T>(toArray stream)


    let inline sortBy (projection : 'T -> 'Key) (stream : ParStream<'T>) : 'T [] =
        let keyValueArrays = 
            fold (fun (keyValueArrays : ArrayCollector<'Key> * ArrayCollector<'T>) value -> 
                    let (keyArray, valueArray) = keyValueArrays 
                    keyArray.Add(projection value)
                    valueArray.Add(value) 
                    keyValueArrays)
                (fun leftKeyValueArrays rightKeyValueArrays ->
                    let (leftKeyArray, leftValueArray) = leftKeyValueArrays
                    let (rightKeyArray, rightValueArray) = rightKeyValueArrays
                    leftKeyArray.AddRange(rightKeyArray)
                    leftValueArray.AddRange(rightValueArray)
                    leftKeyValueArrays) 
                (fun () -> (new ArrayCollector<'Key>(), new ArrayCollector<'T>())) stream 
        let (keyArray, valueArray) = keyValueArrays
        let keyArray' = keyArray.ToArray()
        let valueArray' = valueArray.ToArray()
        Sort.parallelSort keyArray' valueArray'
        valueArray'
