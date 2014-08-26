namespace Nessos.Streams.Cloud
open System
open System.Collections.Generic
open Nessos.MBrace

type Collector<'T, 'R> = 
    abstract Iterator : unit -> ('T -> bool)
    abstract Result : 'R

type CloudStream<'T> = 
    abstract Apply<'R> : Collector<'T, 'R> -> ('R -> 'R -> 'R) -> Cloud<'R>

module CloudStream =

    // generator functions
    let ofArray (source : 'T []) : CloudStream<'T> =
        raise <| new NotImplementedException()


    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.Apply<'S> (collector : Collector<'R, 'S>) combiner =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> iter (f value))
                        member self.Result = collector.Result  }
                stream.Apply collector combiner }



    let inline filter (predicate : 'T -> bool) (stream : CloudStream<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.Apply<'S> (collector : Collector<'T, 'S>) combiner =
                let collector = 
                    { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let iter = collector.Iterator()
                            (fun value -> if predicate value then iter value else true)
                        member self.Result = collector.Result }
                stream.Apply collector combiner }


    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                        (state : unit -> 'State) (stream : CloudStream<'T>) : Cloud<'State> =

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
            stream.Apply collector combiner

    let inline sum (stream : CloudStream< ^T >) : Cloud< ^T> 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    let inline length (stream : CloudStream<'T>) : Cloud<int> =
        fold (fun acc _  -> 1 + acc) (+) (fun () -> 0) stream