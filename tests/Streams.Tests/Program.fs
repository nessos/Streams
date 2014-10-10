namespace Nessos.Streams.Tests
open System
open Nessos.Streams

module Program = 


    [<EntryPoint>]
    let main argv = 
        let data = [|1..10000000|] |> Array.map (fun i -> int64 <| (i % 100000))
        let start = DateTime.Now
        let result = 
            data
            |> ParStream.ofArray
            |> ParStream.map (fun x -> x + 1L)
            |> ParStream.groupBy id
            |> ParStream.length

        printfn "%A" <| DateTime.Now - start
        0

