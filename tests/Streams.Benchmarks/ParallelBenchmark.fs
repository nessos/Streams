namespace Nessos.Streams.Benchmarks

open System.Linq
open Nessos.Streams
open BenchmarkDotNet.Attributes
open FSharp.Collections.ParallelSeq

[<MemoryDiagnoser>]
type ParallelBenchmark() =

    let data = [|1L..10000000L|] |> Array.map int64

    [<Benchmark(Description = "PLINQ Pipeline", Baseline = true)>]
    member _.PLinqPipeline() =
        data.AsParallel()
            .Where(fun x -> x % 2L = 0L)
            .Select(fun x -> x + 1L)
            .Sum()
        |> ignore

    [<Benchmark(Description = "F# PSeq Pipeline")>]
    member _.PSeqPipeline() =
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