namespace Nessos.Streams.Tests
open System
open Nessos.Streams

module Program = 


    [<EntryPoint>]
    let main argv = 
        let xs, ys = [|3; 0|], [|42|]
        let start = DateTime.Now
        let result = 
            xs |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0) |> Stream.zipWith (fun x y -> x + y) (ys |> Stream.ofArray) |> Stream.toArray

        let r = xs |> Seq.filter (fun x -> x % 2 = 0) |> Seq.zip ys |> Seq.map (fun (x, y) -> x + y) |> Seq.toArray

        printfn "%A - %A " r result
        0

