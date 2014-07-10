namespace Nessos.Streams.Core
open System.Collections.Generic
open System.Linq

module Stream =
    
    type Stream<'T> = ('T -> bool) -> unit
        
    // generator functions
    let inline ofArray (source : 'T []) : Stream<'T> =
        (fun iterf -> 
            let mutable i = 0
            let mutable next = true
            while i < source.Length && next do
                next <- iterf source.[i]
                i <- i + 1)

    let inline ofSeq (source : seq<'T>) : Stream<'T> =
        (fun iterf -> 
            use enumerator = source.GetEnumerator()
            let mutable next = true
            while enumerator.MoveNext() && next do
                next <- iterf enumerator.Current)

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        (fun iterf ->
            stream (fun value -> iterf (f value)))

    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        (fun iterf -> 
            stream (fun value -> f value iterf; true))

    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        (fun iterf -> 
            stream (fun value -> if predicate value then iterf value else true))

    let inline take (n : int) (stream : Stream<'T>) : Stream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        (fun iterf -> 
            let counter = ref 0
            stream (fun value -> 
                incr counter
                if !counter <= n then iterf value else false))

    let inline skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        (fun iterf -> 
            let counter = ref 0
            stream (fun value -> 
                incr counter
                if !counter > n then iterf value else true))
        
    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State = 
        let accRef = ref state
        stream (fun value -> accRef := folder !accRef value ; true) 
        !accRef

    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) LanguagePrimitives.GenericZero stream

    let inline length (stream : Stream<'T>) : int =
        fold (fun acc _  -> 1 + acc) 0 stream

    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        stream (fun value -> f value; true) 

    let inline toArray (stream : Stream<'T>) : 'T[] =
        let list = 
            fold (fun (acc : List<'T>) value -> acc.Add(value); acc) (new List<'T>()) stream 
        list.ToArray()

    let inline sortBy (projection : 'T -> 'Key) (stream : Stream<'T>) : 'T [] =
        let array = toArray stream
        Array.sortInPlaceBy projection array
        array

    let inline groupBy (projection : 'T -> 'Key) (stream : Stream<'T>) : seq<'Key * seq<'T>>  =
        let array = toArray stream
        Seq.groupBy projection array
        