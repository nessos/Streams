#time

#r "bin/Release/Streams.Core.dll"

open Nessos.Streams.Core


let data = [|1..10000000|] |> Array.map int64

data
|> Seq.filter (fun x -> x % 2L = 0L)
|> Seq.map (fun x -> x + 1L)
|> Seq.sum


data
|> Array.filter (fun x -> x % 2L = 0L)
|> Array.map (fun x -> x + 1L)
|> Array.sum


data
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2L = 0L)
|> Stream.map (fun x -> x + 1L)
|> Stream.sum

let baseline () = 
    let mutable acc = 0L
    for i = 0 to data.Length - 1 do
        let x = data.[i]
        if x % 2L = 0L then
            acc <- acc + (x + 1L)
    acc

baseline()
