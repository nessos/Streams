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
        member __.``ofResizeArray/toResizeArray`` () =
            Spec.ForAny<ResizeArray<int>>(fun xs ->
                let x = xs |> ParStream.ofResizeArray |> ParStream.map ((+)1) |> ParStream.toResizeArray
                let y = xs |> PSeq.map ((+)1) |> PSeq.toArray
                (x.ToArray()) = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``ofSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofSeq |> ParStream.map ((+)1) |> ParStream.toArray
                let y = xs |> PSeq.map ((+)1) |> PSeq.toArray
                set x = set y).QuickCheckThrowOnFailure()
            
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


        [<Test>]
        member __.``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.filter (fun n -> n % 2 = 0) |> ParStream.length
                let y = xs |> PSeq.filter (fun n -> n % 2 = 0) |> PSeq.length
                x = y).QuickCheckThrowOnFailure()



        [<Test>]
        member __.``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map ((+) 1) |> ParStream.sortBy id |> ParStream.toArray
                let y = xs |> PSeq.map ((+) 1) |> PSeq.sortBy id |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``groupBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs 
                        |> ParStream.ofArray 
                        |> ParStream.groupBy id  
                        |> ParStream.map (fun (key, values) -> (key, values |> Seq.length))
                        |> ParStream.toArray
                let y = xs  
                        |> PSeq.groupBy id 
                        |> PSeq.map (fun (key, values) -> (key, values |> Seq.length))
                        |> PSeq.toArray
                (x |> Array.sortBy fst) = (y |> Array.sortBy fst)).QuickCheckThrowOnFailure()

       
