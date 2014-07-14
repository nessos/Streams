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


open System.Linq

data
    .AsParallel()
    .Where(fun x -> x % 2L = 0L)
    .Select(fun x -> x + 1L)
    .Sum()

