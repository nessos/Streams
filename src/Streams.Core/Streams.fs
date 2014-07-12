namespace Nessos.Streams.Core
open System.Collections.Generic
open System.Linq

module Stream =
    
    type Size = int option
    type Stream<'T> = Stream of (('T -> bool) -> unit) * Size
        
    // generator functions
    let inline ofArray (source : 'T []) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Length && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream (iter, Some source.Length)

    let inline ofResizeArray (source : ResizeArray<'T>) : Stream<'T> =
        let iter iterf =
            let mutable i = 0
            let mutable next = true
            while i < source.Count && next do
                next <- iterf source.[i]
                i <- i + 1
        Stream (iter, Some source.Count)

    let inline ofSeq (source : seq<'T>) : Stream<'T> =
        let iter iterf = 
            use enumerator = source.GetEnumerator()
            let mutable next = true
            while enumerator.MoveNext() && next do
                next <- iterf enumerator.Current
        Stream (iter, None)

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        let (Stream (streamf, size)) = stream
        let iter iterf =
            streamf (fun value -> iterf (f value))
        Stream (iter, size)

    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        let (Stream (streamf, size)) = stream
        let iter iterf =
            streamf (fun value -> 
                        let (Stream (streamf', _)) = f value;
                        streamf' iterf; true)
        Stream (iter, None)

    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    let inline filter (predicate : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        let (Stream (streamf, size)) = stream
        let iter iterf = 
            streamf (fun value -> if predicate value then iterf value else true)
        Stream (iter, None)

    let inline take (n : int) (stream : Stream<'T>) : Stream<'T> =
        if n < 0 then
            raise <| new System.ArgumentException("The input must be non-negative.")
        let (Stream (streamf, size)) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter <= n then iterf value else false)
        Stream (iter, Some n)

    let inline skip (n : int) (stream : Stream<'T>) : Stream<'T> =
        let (Stream (streamf, size)) = stream
        let iter iterf = 
            let counter = ref 0
            streamf (fun value -> 
                incr counter
                if !counter > n then iterf value else true)
        Stream (iter, None)

    // terminal functions
    let inline fold (folder : 'State -> 'T -> 'State) (state : 'State) (stream : Stream<'T>) : 'State =
        let (Stream (streamf, size)) = stream 
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
        let (Stream (streamf, size)) = stream
        streamf (fun value -> f value; true) 

    let inline toResizeArray (stream : Stream<'T>) : ResizeArray<'T> =
        let (Stream (_, size)) = stream
        let list = 
            match size with
            | Some n -> new List<'T>(n)
            | None -> new List<'T>()
        let list = 
            fold (fun (acc : List<'T>) value -> acc.Add(value); acc) (list) stream 
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
        