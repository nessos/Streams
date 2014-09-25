namespace Nessos.Streams.Cloud.Tests
    open System.Threading
    open System.Linq
    open System.Collections.Generic
    open FsCheck
    open FsCheck.Fluent
    open NUnit.Framework
    open Nessos.Streams.Core
    open Nessos.Streams.Cloud
    open Nessos.MBrace
    open Nessos.MBrace.Client

    [<TestFixture>]
    type ``CloudStreams tests`` () =
        do 
            ThreadPool.SetMinThreads(200, 200) |> ignore

        let run (cloud : Cloud<'T>) = MBrace.RunLocal cloud

        [<Test>]
        member __.``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.length |> run
                let y = xs |> Seq.map ((+)1) |> Seq.length
                Assert.AreEqual(y, int x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``ofCloudArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = run <| CloudArray.New("temp", xs) 
                let x = cloudArray |> CloudStream.ofCloudArray |> CloudStream.length |> run
                let y = xs |> Seq.map ((+)1) |> Seq.length
                Assert.AreEqual(y, int x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``toCloudArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map ((+)1) |> CloudStream.toCloudArray |> run
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.toArray |> run
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.filter (fun n -> n % 2 = 0) |> CloudStream.toArray |> run
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``collect`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> CloudStream.toArray |> run
                let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``fold`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.fold (+) (+) (fun () -> 0) |> run
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()  

        [<Test>]
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.sum |> run
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.filter (fun n -> n % 2 = 0) |> CloudStream.length |> run
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
                Assert.AreEqual(y, int x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``countBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.countBy id |> CloudStream.toArray |> run
                let y = xs |> Seq.countBy id |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.sortBy id 10 |> CloudStream.toArray |> run
                let y = (xs |> Seq.sortBy id).Take(10).ToArray()
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()