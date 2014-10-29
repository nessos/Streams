#time

#r "../../bin/Streams.Core.dll"
#r "../../bin/Streams.CSharp.dll"

open System
open System.Linq
open Nessos.Streams
open Nessos.Streams.CSharp


let data = [|1..100000000|] |> Array.map (fun i -> int64 <| (i % 1000000))


data
|> Stream.ofArray
|> Stream.maxBy id

data
|> ParStream.ofArray
|> ParStream.maxBy id

data
|> ParStream.ofArray
|> ParStream.minBy id

data
|> Stream.ofArray
|> Stream.minBy id

data
|> Seq.filter (fun x -> x % 2L = 0L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.map (fun x -> x + 1L)
|> Seq.sum

data
|> Array.filter (fun x -> x % 2L = 0L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.map (fun x -> x + 1L)
|> Array.sum

data
|> Stream.ofArray
|> Stream.filter (fun x -> x % 2L = 0L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.map (fun x -> x + 1L)
|> Stream.sum

data
|> ParStream.ofArray
|> ParStream.filter (fun x -> x % 2L = 0L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.map (fun x -> x + 1L)
|> ParStream.sum


data.AsParallel()
    .Where(fun x -> x % 2L = 0L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Sum()


data.AsParStream()
    .Where(fun x -> x % 2L = 0L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Select(fun x -> x + 1L)
    .Sum()



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






