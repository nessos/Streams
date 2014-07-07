namespace Nessos.Streams.Core
open System.Collections.Generic
open System.Linq

module Stream =
    
    type Stream<'T> = (('T -> unit) -> unit) 
        
    // generator functions
    let inline ofArray (values : 'T []) : Stream<'T> =
        (fun iterf -> 
                for i = 0 to values.Length - 1 do
                    iterf values.[i])

    let inline ofSeq (values : seq<'T>) : Stream<'T> =
        (fun iterf -> 
                for value in values do
                    iterf value)

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
            stream (fun value -> if p value then iterf value))
        
    // terminal functions
    let inline reduce (reducef : 'T -> 'R -> 'R) (init : 'R) (stream : Stream<'T>) : 'R = 
        let accRef = ref init
        stream (fun value -> accRef := reducef value !accRef)
        !accRef

    let inline sum (stream : Stream< ^T >) : ^T 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        reduce (+) LanguagePrimitives.GenericZero stream

    let inline length (stream : Stream<'T>) : int =
        reduce (fun _ acc -> 1 + acc) 0 stream

    let inline iter (f : 'T -> unit) (stream : Stream<'T>) : unit = 
        stream f

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
        