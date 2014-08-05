#time

#r "bin/Release/Streams.Core.dll"

open Nessos.Streams.Core


let data = [|1..10000000|] |> Array.map (fun i -> int64 <| (i % 1000000))

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





#r "../../packages/FSharp.Collections.ParallelSeq.1.0/lib/net40/FSharp.Collections.ParallelSeq.dll"
open FSharp.Collections.ParallelSeq

data
|> PSeq.map (fun x -> x + 1L)
|> PSeq.groupBy id
|> PSeq.length

data
|> ParStream.ofArray
|> ParStream.map (fun x -> x + 1L)
|> ParStream.groupBy id
|> ParStream.length






