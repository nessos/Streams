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
                    let tasks = partitions |> Array.mapi (fun index (s, e) -> 
                                                            let iter = collector.Iterator()
                                                            createTask s e iter)

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

    let toArray (stream : ParStream<'T>) : 'T[] =
        let arrayCollector = 
            fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                (fun left right -> left.AddRange(right); left) 
                (fun () -> new ArrayCollector<'T>()) stream 
        arrayCollector.ToArray()
