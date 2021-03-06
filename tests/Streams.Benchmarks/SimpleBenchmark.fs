﻿namespace Nessos.Streams.Benchmarks

open System.Linq
open Nessos.Streams
open BenchmarkDotNet.Attributes
open FSharp.Collections.ParallelSeq

[<MemoryDiagnoser>]
type SimpleBenchmarkSeq() =

    let data = [|1L..10000000L|]

    [<Benchmark(Description = "LINQ Pipeline", Baseline = true)>]
    member _.LinqPipeline() =
        data.Where(fun x -> x % 2L = 0L)
            .Select(fun x -> x + 1L)
            .Sum()
        |> ignore

    [<Benchmark(Description = "F# Seq Pipeline")>]
    member _.SeqPipeline() =
        data
        |> Seq.filter (fun x -> x % 2L = 0L)
        |> Seq.map (fun x -> x + 1L)
        |> Seq.sum
        |> ignore
    
    [<Benchmark(Description = "Nessos Stream Pipeline")>]
    member _.StreamPipeline() =
        data
        |> Stream.ofArray
        |> Stream.filter (fun x -> x % 2L = 0L)
        |> Stream.map (fun x -> x + 1L)
        |> Stream.sum
        |> ignore

[<MemoryDiagnoser>]
type SimpleBenchmarkParallel() =

    let data = [|1L..10000000L|]

    [<Benchmark(Description = "PLINQ Pipeline", Baseline = true)>]
    member _.PLinqPipeline() =
        data.AsParallel()
            .Where(fun x -> x % 2L = 0L)
            .Select(fun x -> x + 1L)
            .Sum()
        |> ignore

    [<Benchmark(Description = "F# PSeq Pipeline")>]
    member _.PSeqPipeline() =
        let mutable count = 0
        data
        |> PSeq.filter (fun x -> x % 2L = 0L)
        |> PSeq.map (fun x -> x + 1L)
        |> PSeq.sum
        |> ignore
    
    [<Benchmark(Description = "Nessos ParStream Pipeline")>]
    member _.ParStreamPipeline() =
        data
        |> ParStream.ofArray
        |> ParStream.filter (fun x -> x % 2L = 0L)
        |> ParStream.map (fun x -> x + 1L)
        |> ParStream.sum
        |> ignore
