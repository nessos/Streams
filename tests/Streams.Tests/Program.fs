namespace Nessos.Streams.Tests
open Nessos.Streams.Core

module Program = 


    [<EntryPoint>]
    let main argv = 
        let data = [|1..10|] |> Array.map int64
        let result = 
            data
            |> Stream.ofArray
            |> Stream.toArray
        0

