namespace Nessos.Streams.Tests
    open System.Linq
    open System.Threading
    open System.Collections.Generic
    open FsCheck
    open FsCheck.Fluent
    open NUnit.Framework
    open FSharp.Collections.ParallelSeq
    open Nessos.Streams

    [<TestFixture; Category("ParStreams.FSharp")>]
    type ``ParStreams tests`` () =

        [<OneTimeSetUp>]
        member __.SetUp() =
            System.Threading.ThreadPool.SetMinThreads(200, 200) |> ignore

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
        member __.``toSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofSeq |> ParStream.map ((+)1) |> ParStream.toSeq
                let y = xs |> PSeq.map ((+)1) 
                set x = set y).QuickCheckThrowOnFailure()
            
        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map (fun n -> 2 * n) |> ParStream.toArray
                let y = xs |> PSeq.map (fun n -> 2 * n) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``mapi`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.filter (fun x -> x % 2 = 0) |> ParStream.flatMap (fun _ -> Stream.ofArray xs) |> ParStream.mapi (fun i n -> (i, n)) |> ParStream.toArray
                let y = xs |> Seq.filter (fun x -> x % 2 = 0) |> Seq.collect (fun _ -> xs) |> Seq.mapi (fun i n -> (i, n)) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.filter (fun n -> n % 2 = 0) |> ParStream.toArray
                let y = xs |> PSeq.filter (fun n -> n % 2 = 0) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``choose`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.choose (fun n -> if n % 2 = 0 then Some n else None) |> ParStream.toArray
                let y = xs |> PSeq.choose (fun n -> if n % 2 = 0 then Some n else None) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``flatMap`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> ParStream.toArray
                let y = xs |> PSeq.collect (fun n -> [|1..n|]) |> PSeq.toArray
                x = y).QuickCheckThrowOnFailure()

        
        [<Test>]
        member __.``flatMap/find`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = Seq.initInfinite id |> ParStream.ofSeq |> ParStream.flatMap (fun x -> Seq.initInfinite id |> Stream.ofSeq) |> ParStream.map ((+)1) |> ParStream.find (fun i -> i = 100)
                let y = Seq.initInfinite id |> Seq.collect (fun x -> Seq.initInfinite id) |> Seq.map ((+)1) |> Seq.find (fun i -> i = 100)
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``flatMap/take`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = Seq.initInfinite id |> ParStream.ofSeq |> ParStream.flatMap (fun x -> Seq.initInfinite id |> Stream.ofSeq |> Stream.take 10) |> ParStream.map ((+)1) |> ParStream.take 100 |> ParStream.toArray
                let y = Seq.initInfinite id |> Seq.collect (fun x -> Seq.initInfinite id |> Seq.take 10) |> Seq.map ((+)1) |> Seq.take 100 |> Seq.toArray 
                x.Length = y.Length).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``fold`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map (fun n -> 2 * n) |> ParStream.fold (+) (+) (fun () -> 0) 
                let y = xs |> PSeq.map (fun n -> 2 * n) |> PSeq.fold (+) 0 
                x = y).QuickCheckThrowOnFailure()  

        [<Test>]
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map (fun n -> 2 * n) |> ParStream.sum
                let y = xs |> PSeq.map (fun n -> 2 * n) |> PSeq.sum
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``sum/ordered`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.filter (fun x -> x % 2 = 0) |> ParStream.mapi (fun i x -> i + x) |> ParStream.sum
                let y = xs |> PSeq.filter (fun x -> x % 2 = 0) |> PSeq.mapi (fun i x -> i + x) |> PSeq.sum
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.filter (fun n -> n % 2 = 0) |> ParStream.length
                let y = xs |> PSeq.filter (fun n -> n % 2 = 0) |> PSeq.length
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``take/unordered`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) ->
                let n = System.Math.Abs(n) 
                let x = xs |> ParStream.ofArray |> ParStream.take n |> ParStream.length
                let y = xs.Take(n).Count()
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``take/ordered`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) ->
                let n = System.Math.Abs(n) 
                let x = xs |> ParStream.ofArray |> ParStream.sortBy id |> ParStream.take n |> ParStream.toArray
                let y = xs.OrderBy(fun x -> x).Take(n).ToArray()
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``skip/unordered`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) -> 
                let x = xs |> ParStream.ofArray |> ParStream.skip n |> ParStream.length
                let y = xs.Skip(n).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``skip/ordered`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) -> 
                let x = xs |> ParStream.ofArray |> ParStream.sortBy id |> ParStream.skip n |> ParStream.toArray
                let y = xs.OrderBy(fun x -> x).Skip(n).ToArray()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.map ((+) 1) |> ParStream.sortBy id |> ParStream.toArray
                let y = xs |> Seq.map ((+) 1) |> Seq.sortBy id |> Seq.toArray
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

        [<Test>]
        member __.``tryFind`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.tryFind (fun n -> n = 0) 
                let y = xs |> PSeq.tryFind (fun n -> n = 0) 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``find`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = try xs |> ParStream.ofArray |> ParStream.find (fun n -> n = 0) with | :? KeyNotFoundException -> -1
                let y = try xs |> PSeq.find (fun n -> n = 0) with | :? System.InvalidOperationException -> -1
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``tryPick`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.tryPick (fun n -> if n = 0 then Some n else None) 
                let y = xs |> Seq.tryPick (fun n -> if n = 0 then Some n else None) 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``pick`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = try xs |> ParStream.ofArray |> ParStream.pick (fun n -> if n = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                let y = try xs |> PSeq.pick (fun n -> if n = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``exists`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.exists (fun n -> n % 2 = 0) 
                let y = xs |> PSeq.exists (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``forall`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> ParStream.ofArray |> ParStream.forall (fun n -> n % 2 = 0) 
                let y = xs |> PSeq.forall (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``foldBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs 
                        |> ParStream.ofArray 
                        |> ParStream.foldBy id (fun ts t -> t :: ts)  (fun () -> []) // groupBy implementation
                        |> ParStream.map (fun (key, values) -> (key, values |> Seq.length))
                        |> ParStream.toArray
                let y = xs  
                        |> Seq.groupBy id 
                        |> Seq.map (fun (key, values) -> (key, values |> Seq.length))
                        |> Seq.toArray
                set x = set y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``countBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.countBy (fun i -> i % 5) |> Stream.toArray
                let y = xs |> Seq.countBy (fun i -> i % 5) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``minBy`` () =
            Spec.ForAny<int[]>(fun xs -> 
                if Array.isEmpty xs then
                    try let _ = xs |> ParStream.ofArray |> ParStream.minBy (fun i -> i + 1) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> ParStream.ofArray |> ParStream.minBy (fun i -> i + 1)
                    let y = xs |> Seq.minBy (fun i -> i + 1)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``maxBy`` () =
            Spec.ForAny<int[]>(fun xs -> 
                if Array.isEmpty xs then 
                    try let _ = xs |> ParStream.ofArray |> ParStream.maxBy (fun i -> i + 1) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> ParStream.ofArray |> ParStream.maxBy (fun i -> i + 1)
                    let y = xs |> Seq.maxBy (fun i -> i + 1)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``reduce``() =
            Spec.ForAny<int []>(fun (xs : int  []) ->
                if Array.isEmpty xs then
                    try let _ = xs |> ParStream.ofArray |> ParStream.reduce (+) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> ParStream.ofArray |> ParStream.reduce (+)
                    let y = xs |> Stream.ofArray |> Stream.reduce (+)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``averageBy``() =
            Spec.ForAny<int []>(fun (xs : int  []) ->
                if Array.isEmpty xs then
                    try let _ = xs |> ParStream.ofArray |> ParStream.averageBy (float) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> ParStream.ofArray |> ParStream.averageBy (float)
                    let y = xs |> Stream.ofArray |> Stream.averageBy (float)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``average``() =
            Spec.ForAny<double []>(fun (xs : double []) ->
                if Array.isEmpty xs then
                    try let _ = xs |> ParStream.ofArray |> ParStream.average in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> ParStream.ofArray |> ParStream.average
                    let y = xs |> Array.average
                    if System.Double.IsNaN x then System.Double.IsNaN y
                    elif System.Double.IsNaN y then System.Double.IsNaN x
                    elif System.Double.IsPositiveInfinity x then System.Double.IsPositiveInfinity y
                    elif System.Double.IsPositiveInfinity y then System.Double.IsPositiveInfinity x
                    elif System.Double.IsNegativeInfinity x then System.Double.IsNegativeInfinity y
                    elif System.Double.IsNegativeInfinity y then System.Double.IsNegativeInfinity x
                    else System.Math.Abs(x - y) < 0.001).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``withDegreeOfParallelism`` () =
            Spec.ForAny<int[]>(fun xs -> 
                let x = xs 
                        |> ParStream.ofArray
                        |> ParStream.map (fun _ -> Thread.CurrentThread.ManagedThreadId) 
                        |> ParStream.withDegreeOfParallelism 1
                        |> ParStream.toArray
                        |> Set.ofArray
                        |> Seq.length
                if xs.Length = 0 then x = 0
                else x = 1 ).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``ordered/foldBy`` () =
            Spec.ForAny<(int * int) []>(fun xs -> 
                let x = xs 
                        |> ParStream.ofSeq
                        |> ParStream.ordered
                        |> ParStream.foldBy fst (fun l x -> x :: l) (fun () -> [])
                        |> ParStream.map (fun (k, vs) -> (k, List.rev vs))
                        |> ParStream.toArray
                let y = xs
                        |> Seq.groupBy fst
                        |> Seq.map (fun (k, vs) -> (k, List.ofSeq vs))
                        |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``head`` () =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x =
                    try xs |> ParStream.ofArray |> ParStream.head
                    with :? System.ArgumentException -> -1
                let y =
                    try xs |> PSeq.ofArray |> PSeq.head
                    with :? System.InvalidOperationException -> -1
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``tryHead`` () =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x = xs |> ParStream.ofArray |> ParStream.tryHead
                let y = xs |> Stream.ofArray |> Stream.tryHead
                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``isEmpty``() =
            Spec.ForAny<int []>(fun (xs : int  []) ->
                let x = xs |> ParStream.ofArray |> ParStream.isEmpty
                let y = xs |> Stream.ofArray |> Stream.isEmpty

                Assert.AreEqual(x, y)).QuickCheckThrowOnFailure()
