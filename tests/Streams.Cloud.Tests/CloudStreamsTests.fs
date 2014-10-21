namespace Nessos.Streams.Cloud.Tests
    #nowarn "0444" // Disable mbrace warnings
    
    open System.Threading
    open System.Linq
    open FsCheck.Fluent
    open NUnit.Framework
    open Nessos.Streams
    open Nessos.Streams.Cloud
    open Nessos.MBrace
    open Nessos.MBrace.Client
    open System.IO

    [<TestFixture; AbstractClass>]
    type ``CloudStreams tests`` () =
        do 
            ThreadPool.SetMinThreads(200, 200) |> ignore

        abstract Evaluate : Cloud<'T> -> 'T

        [<Test>]
        member __.``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.length |> __.Evaluate
                let y = xs |> Seq.map ((+)1) |> Seq.length
                Assert.AreEqual(y, int x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``ofCloudArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = __.Evaluate <| CloudArray.New("temp", xs) 
                let x = cloudArray |> CloudStream.ofCloudArray |> CloudStream.length |> __.Evaluate
                let y = xs |> Seq.map ((+)1) |> Seq.length
                Assert.AreEqual(y, int x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``toCloudArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map ((+)1) |> CloudStream.toCloudArray |> __.Evaluate
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``cache`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = __.Evaluate <| CloudArray.New("temp", xs) 
                let cached = CloudStream.cache cloudArray |> __.Evaluate 
                let x = cached |> CloudStream.ofCloudArray |> CloudStream.map  (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let x' = cached |> CloudStream.ofCloudArray |> CloudStream.map (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let y = xs |> Seq.map (fun x -> x * x) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())
                Assert.AreEqual(x'.ToArray(), x.ToArray())).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``subsequent caching`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = __.Evaluate <| CloudArray.New("temp", xs) 
                let _ = CloudStream.cache cloudArray |> __.Evaluate 
                let cached = CloudStream.cache cloudArray |> __.Evaluate 
                let x = cached |> CloudStream.ofCloudArray |> CloudStream.map  (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let x' = cached |> CloudStream.ofCloudArray |> CloudStream.map (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let y = xs |> Seq.map (fun x -> x * x) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())
                Assert.AreEqual(x'.ToArray(), x.ToArray())).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``ofCloudFiles`` () =
            Spec.ForAny<string []>(fun (xs : string []) ->
                let cfs = 
                    xs |> Array.map(fun text -> 
                        StoreClient.Default.CreateCloudFile(System.Guid.NewGuid().ToString(),
                            (fun (stream : Stream) -> 
                                async {
                                    use sw = new StreamWriter(stream)
                                    sw.Write(text) })))

                let x = cfs |> CloudStream.ofCloudFiles CloudFile.ReadAllText
                            |> CloudStream.toArray
                            |> __.Evaluate
                            |> Set.ofArray

                let y = cfs |> Array.map (fun cf -> cf.Read())
                            |> Array.map (fun s -> async { let! s = s in return! CloudFile.ReadAllText s })
                            |> Async.Parallel
                            |> Async.RunSynchronously
                            |> Set.ofArray

                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.filter (fun n -> n % 2 = 0) |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``collect`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``fold`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.fold (+) (+) (fun () -> 0) |> __.Evaluate
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()  

        [<Test>]
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.sum |> __.Evaluate
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.filter (fun n -> n % 2 = 0) |> CloudStream.length |> __.Evaluate
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
                Assert.AreEqual(y, int x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``countBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.countBy id |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.countBy id |> Seq.map (fun (k,c) -> k, int64 c) |> Seq.toArray
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.sortBy id 10 |> CloudStream.toArray |> __.Evaluate
                let y = (xs |> Seq.sortBy id).Take(10).ToArray()
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()


    [<Category("CloudStreams.RunLocal")>]
    type ``#1 RunLocal Tests`` () =
        inherit ``CloudStreams tests`` ()

        override __.Evaluate(expr : Cloud<'T>) : 'T = MBrace.RunLocal expr

    [<Category("CloudStreams.Cluster")>]
    type ``#2 Cluster Tests`` () =
        inherit ``CloudStreams tests`` ()
        
        let currentRuntime : MBraceRuntime option ref = ref None
        
        override __.Evaluate(expr : Cloud<'T>) : 'T = currentRuntime.Value.Value.Run expr

        [<TestFixtureSetUp>]
        member test.InitRuntime() =
            lock currentRuntime (fun () ->
                match currentRuntime.Value with
                | Some runtime -> runtime.Kill()
                | None -> ()
            
                let ver = typeof<MBrace>.Assembly.GetName().Version.ToString(3)
                MBraceSettings.MBracedExecutablePath <- Path.Combine(__SOURCE_DIRECTORY__, "../../packages/MBrace.Runtime." + ver + "-alpha/tools/mbraced.exe")
                MBraceSettings.DefaultTimeout <- 60 * 1000
                let runtime = MBraceRuntime.InitLocal(3)
                currentRuntime := Some runtime)

        [<TestFixtureTearDown>]
        member test.FiniRuntime() =
            lock currentRuntime (fun () -> 
                match currentRuntime.Value with
                | None -> invalidOp "No runtime specified in test fixture."
                | Some r -> r.Shutdown() ; currentRuntime := None)