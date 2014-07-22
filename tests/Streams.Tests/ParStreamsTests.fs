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
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map (fun n -> 2 * n) |> ParStream.sum
                let y = xs |> PSeq.map (fun n -> 2 * n) |> PSeq.sum
                x = y).QuickCheckThrowOnFailure()

       
