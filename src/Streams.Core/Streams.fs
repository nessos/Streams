namespace Nessos.Streams.Core
open System.Collections.Generic
open System.Linq

module Stream =
    
    type Stream<'T> = (('T -> bool) -> bool) 
        
    // generator functions
    let inline ofArray (values : 'T []) : Stream<'T> =
        (fun iterf -> 
                let mutable i = 0
                let mutable next = true
                while i < values.Length && next do
                    next <- iterf values.[i]
                    i <- i + 1
                next)

    let inline ofSeq (values : seq<'T>) : Stream<'T> =
        (fun iterf -> 
                use enumerator = values.GetEnumerator()
                let mutable next = true
                while enumerator.MoveNext() && next do
                    next <- iterf enumerator.Current
                next)

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        (fun iterf -> 
            stream (fun value -> iterf (f value)))

    let inline flatMap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        (fun iterf -> 
            stream (fun value -> f value iterf))

    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatMap f stream

    let inline filter (p : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        (fun iterf -> 
            stream (fun value -> if p value then iterf value else true))

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
    let inline reduce (reducef : 'T -> 'R -> 'R) (init : 'R) (stream : Stream<'T>) : 'R = 
        let accRef = ref init
        stream (fun value -> accRef := reducef value !accRef; true) |> ignore
        !accRef

    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        reduce (+) LanguagePrimitives.GenericZero stream

    let inline length (stream : Stream<'T>) : int =
        reduce (fun _ acc -> 1 + acc) 0 stream

    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        stream (fun value -> f value; true) |> ignore

    let inline toArray (stream : Stream<'T>) : 'T[] =
        let list = 
            reduce (fun value (acc : List<'T>) -> acc.Add(value); acc) (new List<'T>()) stream 
        list.ToArray()

    let inline sortBy (f : 'T -> 'Key) (stream : Stream<'T>) : 'T [] =
        let array = toArray stream
        Array.sortInPlaceBy f array
        array

    let inline groupBy (f : 'T -> 'Key) (stream : Stream<'T>) : seq<'Key * seq<'T>>  =
        let array = toArray stream
        Seq.groupBy f array
        