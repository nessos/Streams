namespace Nessos.Streams.Tests
    open FsCheck
    open FsCheck.Fluent
    open NUnit.Framework
    open Nessos.Streams.Core

    [<TestFixture>]
    type ``Streams tests`` () =

        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.toArray
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun n -> n % 2 = 0) |> Stream.toArray
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()
            

