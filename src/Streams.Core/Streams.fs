namespace Nessos.Streams.Core
open System.Collections.Generic
open System.Linq


type Stream<'T> = Stream of (('T -> bool) -> unit)
module Stream =
           
    // generator functions
    let inline ofArray (source : 'T []) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Length && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream iter

    let inline ofResizeArray (source : ResizeArray<'T>) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Count && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream iter

    let inline ofSeq (source : seq<'T>) : Stream<'T> =
        let iter iterf = 
            use enumerator = source.GetEnumerator()
            let mutable next = true
            while enumerator.MoveNext() && next do
                next <- iterf enumerator.Current
        Stream iter

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf =
            streamf (fun value -> iterf (f value))
        Stream iter

    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        let (Stream streamf) = stream
        let iter iterf =
            streamf (fun value -> 
                        let (Stream streamf') = f value;
                        streamf' iterf; true)
        Stream iter

    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> if predicate value then iterf value else true)
        Stream iter

    let inline take (n : int) (stream : Stream<'T>) : Stream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        let (Stream streamf) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter <= n then iterf value else false)
        Stream iter

    let inline takeWhile pred (stream : Stream<'T>) : Stream<'T> = 
        let (Stream streamf) = stream
        let iter iterf = 
            streamf (fun value -> 
                if pred value then iterf value else false)
        Stream iter

    let inline skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        let (Stream streamf) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter > n then iterf value else true)
        Stream iter

    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State =
        let (Stream streamf) = stream 
        let accRef = ref state
        streamf (fun value -> accRef := folder !accRef value ; true) 
        !accRef

    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) LanguagePrimitives.GenericZero stream

    let inline length (stream : Stream<'T>) : int =
        fold (fun acc _  -> 1 + acc) 0 stream

    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        let (Stream streamf) = stream
        streamf (fun value -> f value; true) 

    let inline toResizeArray (stream : Stream<'T>) : ResizeArray<'T> =
        let (Stream _) = stream
        let list = 
            fold (fun (acc : List<'T>) value -> acc.Add(value); acc) (new List<'T>()) stream 
        list

    let inline toArray (stream : Stream<'T>) : 'T[] =
        let list = toResizeArray stream
        list.ToArray()

    let inline sortBy (projection : 'T -> 'Key) (stream : Stream<'T>) : 'T [] =
        let array = toArray stream
        Array.sortInPlaceBy projection array
        array

    let inline groupBy (projection : 'T -> 'Key) (stream : Stream<'T>) : seq<'Key * seq<'T>>  =
        let array = toArray stream
        Seq.groupBy projection array
        