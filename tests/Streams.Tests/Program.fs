namespace Nessos.Streams.Tests
open Nessos.Streams.Core

module Program = 


    [<EntryPoint>]
    let main argv = 
        let data = [|1..1000000|] |> Array.map int64
        let result = 
            data
            |> Stream.ofArray
            |> Stream.filter (fun x -> x % 2L = 0L)
            |> Stream.map (fun x -> x + 1L)
            |> Stream.sum
        0

