namespace Nessos.Streams.Cloud
open System


type Collector<'T, 'R> = 
    abstract Iterator : unit -> ('T -> bool)
    abstract Result : 'R

type CloudStream<'T> = 
    abstract Apply<'R> : Collector<'T, 'R> -> unit

module CloudStream =

    // generator functions
    let ofArray (source : 'T []) : CloudStream<'T> =
        raise <| new NotImplementedException()