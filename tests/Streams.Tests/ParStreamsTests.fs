namespace Nessos.Streams.Tests
    open System.Linq
    open System.Collections.Generic
    open FsCheck
    open FsCheck.Fluent
    open NUnit.Framework
    open FSharp.Collections.ParallelSeq
    open Nessos.Streams.Core

    [<TestFixture>]
    type ``ParStreams tests`` () =

        [<Test>]
        member __.``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map ((+)1) |> ParStream.toArray
                let y = xs |> PSeq.map ((+)1) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()
            
        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map (fun n -> 2 * n) |> ParStream.toArray
                let y = xs |> PSeq.map (fun n -> 2 * n) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.filter (fun n -> n % 2 = 0) |> ParStream.toArray
                let y = xs |> PSeq.filter (fun n -> n % 2 = 0) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``collect`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> ParStream.toArray
                let y = xs |> PSeq.collect (fun n -> [|1..n|]) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map (fun n -> 2 * n) |> ParStream.sum
                let y = xs |> PSeq.map (fun n -> 2 * n) |> PSeq.sum
                x = y).QuickCheckThrowOnFailure()

       
