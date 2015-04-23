namespace Nessos.Streams.Tests
    open System.Linq
    open System.Collections.Generic
    open FsCheck
    open FsCheck.Fluent
    open NUnit.Framework
    open Nessos.Streams

    [<TestFixture; Category("Streams.FSharp")>]
    type ``Streams tests`` () =

        [<Test>]
        member __.``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map ((+)1) |> Stream.toArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``ofResizeArray/toResizeArray`` () =
            Spec.ForAny<ResizeArray<int>>(fun xs ->
                let x = xs |> Stream.ofResizeArray |> Stream.map ((+)1) |> Stream.toResizeArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                (x.ToArray()) = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``ofSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofSeq |> Stream.map ((+)1) |> Stream.toArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``toSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.flatMap (fun _ -> Stream.ofArray xs) |> Stream.filter (fun x -> x % 2 = 0) |> Stream.map ((+)1) |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.collect (fun _ -> xs) |> Seq.filter (fun x -> x % 2 = 0) |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``flatMap/toSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.flatMap (fun x -> [|1..10|] |> Stream.ofArray) |> Stream.map ((+)1) |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.collect (fun x -> [|1..10|]) |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``flatMap/find`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = Seq.initInfinite id |> Stream.ofSeq |> Stream.flatMap (fun x -> Seq.initInfinite id |> Stream.ofSeq) |> Stream.map ((+)1) |> Stream.find (fun _ -> true)
                let y = Seq.initInfinite id |> Seq.collect (fun x -> Seq.initInfinite id) |> Seq.map ((+)1) |> Seq.find (fun _ -> true)
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``flatMap/take`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = Seq.initInfinite id |> Stream.ofSeq |> Stream.flatMap (fun x -> Seq.initInfinite id |> Stream.ofSeq |> Stream.take 10) |> Stream.map ((+)1) |> Stream.take 100 |> Stream.toArray
                let y = Seq.initInfinite id |> Seq.collect (fun x -> Seq.initInfinite id |> Seq.take 10) |> Seq.map ((+)1) |> Seq.take 100 |> Seq.toArray 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.toArray
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``mapi`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.mapi (fun i n -> i * n) |> Stream.toArray
                let y = xs |> Seq.mapi (fun i n -> i * n) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun n -> n % 2 = 0) |> Stream.toArray
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``choose`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.choose (fun n -> if n % 2 = 0 then Some n else None) |> Stream.toArray
                let y = xs |> Seq.choose (fun n -> if n % 2 = 0 then Some n else None) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``collect`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> Stream.toArray
                let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``fold`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.fold (+) 0 
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``scan`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.scan (+) 0 |> Stream.toArray 
                let y = xs |> Seq.scan (+) 0 |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()            

        [<Test>]
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.sum
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun n -> n % 2 = 0) |> Stream.length
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map ((+) 1) |> Stream.sortBy id |> Stream.toArray
                let y = xs |> Seq.map ((+) 1) |> Seq.sortBy id |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``minBy`` () =
            Spec.ForAny<int[]>(fun xs -> 
                if Array.isEmpty xs then
                    try let _ = xs |> Stream.ofArray |> Stream.minBy (fun i -> i + 1) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.minBy (fun i -> i + 1)
                    let y = xs |> Seq.minBy (fun i -> i + 1)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``maxBy`` () =
            Spec.ForAny<int[]>(fun xs -> 
                if Array.isEmpty xs then 
                    try let _ = xs |> Stream.ofArray |> Stream.maxBy (fun i -> i + 1) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.maxBy (fun i -> i + 1)
                    let y = xs |> Seq.maxBy (fun i -> i + 1)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``countBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.countBy (fun i -> i % 5) |> Stream.toArray
                let y = xs |> Seq.countBy (fun i -> i % 5) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``foldBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs 
                        |> Stream.ofArray 
                        |> Stream.foldBy id (fun ts t -> t :: ts) (fun () -> []) // groupBy implementation
                        |> Stream.map (fun (key, values) -> (key, values |> Seq.length))
                        |> Stream.toArray
                let y = xs  
                        |> Seq.groupBy id 
                        |> Seq.map (fun (key, values) -> (key, values |> Seq.length))
                        |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``groupBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs 
                        |> Stream.ofArray 
                        |> Stream.groupBy id  
                        |> Stream.map (fun (key, values) -> (key, values |> Seq.length))
                        |> Stream.toArray
                let y = xs  
                        |> Seq.groupBy id 
                        |> Seq.map (fun (key, values) -> (key, values |> Seq.length))
                        |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``take`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) ->
                let n = System.Math.Abs(n) 
                let x = xs |> Stream.ofArray |> Stream.take n |> Stream.length
                let y = xs.Take(n).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``take/boundary check`` () =
            Spec.ForAny<int>(fun (n : int) ->
                let refs = [| 1; 2; 3; 4 |]
                let n = System.Math.Abs(n) 
                let x = [|1..n|]
                        |> Stream.ofArray
                        |> Stream.map (fun i -> refs.[i])
                        |> Stream.take 3
                        |> Stream.toArray
                let y = ([|1..n|] |> Seq.map (fun i -> refs.[i])).Take(3).ToArray()
                x = y).QuickCheckThrowOnFailure()
        


                
        [<Test>]
        member __.``takeWhile`` () =
            Spec.ForAny<int[]>(fun xs ->
                let pred = (fun value -> value % 2 = 0)
                let x = xs |> Stream.ofArray |> Stream.takeWhile pred |> Stream.length
                let y = xs.TakeWhile(pred).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``collect/take`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) ->
                let n = System.Math.Abs(n) 
                let x = xs |> Stream.ofArray |> Stream.collect(fun x -> xs |> Stream.ofArray |> Stream.take n) |> Stream.length
                let y = xs.SelectMany(fun x -> xs.Take(n)).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``skip`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) -> 
                let x = xs |> Stream.ofArray |> Stream.skip n |> Stream.length
                let y = xs.Skip(n).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``tryFind`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.tryFind (fun n -> n % 2 = 0) 
                let y = xs |> Seq.tryFind (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``find`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = try xs |> Stream.ofArray |> Stream.find (fun n -> n % 2 = 0) with | :? KeyNotFoundException -> -1
                let y = try xs |> Seq.find (fun n -> n % 2 = 0) with | :? KeyNotFoundException -> -1
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``tryPick`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.tryPick (fun n -> if n % 2 = 0 then Some n else None) 
                let y = xs |> Seq.tryPick (fun n -> if n % 2 = 0 then Some n else None) 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``pick`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = try xs |> Stream.ofArray |> Stream.pick (fun n -> if n % 2 = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                let y = try xs |> Seq.pick (fun n -> if n % 2 = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``exists`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.exists (fun n -> n % 2 = 0) 
                let y = xs |> Seq.exists (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``forall`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.forall (fun n -> n % 2 = 0) 
                let y = xs |> Seq.forall (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()



        [<Test>]
        member __.``zipWith`` () =
            Spec.ForAny<(int[] * int[])>(fun (xs, ys) ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0) |> Stream.zipWith (fun x y -> x + y) (ys |> Stream.ofArray) |> Stream.toArray
                let y = xs |> Seq.filter (fun x -> x % 2 = 0) |> Seq.zip ys |> Seq.map (fun (x, y) -> x + y) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``empty`` () =
            //for bulk
            Assert.AreEqual(0, Stream.empty<int> |> Stream.length)
            //for next
            Assert.AreEqual(0, Stream.empty<int> |> Stream.toSeq |> Seq.length)

        [<Test>]
        member __.``concat`` () =
            Spec.ForAny<int[][]>(fun xs ->
                let x = xs |> Seq.map (fun xs' -> xs' |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0)) |> Stream.concat |> Stream.toArray
                let y = xs |> Seq.map (fun xs' -> xs' |> Seq.filter (fun x -> x % 2 = 0)) |> Seq.concat |> Seq.toArray
                Assert.AreEqual(x, y)
                let x = xs |> Seq.map (fun xs' -> xs' |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0)) |> Stream.concat |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.map (fun xs' -> xs' |> Seq.filter (fun x -> x % 2 = 0)) |> Seq.concat |> Seq.toArray
                Assert.AreEqual(x, y)
                ).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``cast`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |>  Stream.cast |> Stream.toArray
                let y = xs |> Seq.cast |> Seq.toArray
                Assert.AreEqual(x, y)
                let x = xs |>  Stream.cast |> Stream.toSeq |> Seq.length
                let y = xs |> Seq.cast |> Seq.length
                Assert.AreEqual(x, y)
                ).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``cache``() =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.cache |> Stream.toArray
                let y = xs |> Seq.cache |> Seq.toArray
                Assert.AreEqual(x, y)
                let cached = xs |> Stream.ofArray |> Stream.filter(fun x -> x % 2 = 0) |> Stream.cache
                let cachedSeq = xs |> Seq.filter(fun x -> x % 2 = 0) |> Seq.cache
                let x' = cached |> Stream.map (fun x -> x + 1) |> Stream.toArray
                let x'' = cached |> Stream.map (fun x -> x + 1) |> Stream.toArray
                Assert.AreEqual(x', x'')
                let y' = cachedSeq |> Seq.map (fun x -> x + 1) |> Seq.toArray
                Assert.AreEqual(x', y')

                let cached' = xs |> Stream.ofArray |> Stream.filter(fun x -> x % 2 = 0) |> Stream.cache
                let cachedSeq' = xs |> Seq.filter(fun x -> x % 2 = 0) |> Seq.cache

                let count = ref 0
                try cached' |> Stream.map (fun v -> (if !count = 1 then failwith "OOPS"); incr count; v) |> ignore with _ -> ()
                try cachedSeq' |> Seq.map (fun v -> (if !count = 1 then failwith "OOPS"); incr count; v) |> ignore with _ -> ()
                let x''' = cached' |> Stream.map (fun x -> x + 1) |> Stream.toArray
                let y''' = cachedSeq' |> Seq.map (fun x -> x + 1) |> Seq.toArray
                Assert.AreEqual(x''', y''')
                Assert.AreEqual(x''', x'')

                let cached'' = xs |> Stream.ofArray |> Stream.filter(fun x -> x % 2 = 0) |> Stream.cache
                let x4 = cached'' |> Stream.map (fun v -> v + 1) |> Stream.toSeq |> Seq.toArray
                let x5 = cached'' |> Stream.map (fun v -> v + 1) |> Stream.toArray
                Assert.AreEqual(x4, x5)
                ).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``groupUntil``() =
            Spec.ForAny<int []>(fun xs ->
                // push-based tests
                let ys = xs |> Stream.ofArray |> Stream.groupUntil true (fun i -> i % 7 <> 0) |> Stream.toArray |> Array.concat
                Assert.AreEqual(xs, ys)
                let failedPredicates = ref 0
                let ys = xs |> Stream.ofArray |> Stream.groupUntil false (fun i -> if i % 7 <> 0 then true else incr failedPredicates ; false) |> Stream.toArray |> Seq.concat |> Seq.length
                Assert.AreEqual(xs.Length, ys + !failedPredicates)
                // pull-based tests
                let ys = xs |> Stream.ofArray |> Stream.groupUntil true (fun i -> i % 7 <> 0) |> Stream.toSeq |> Array.concat
                Assert.AreEqual(xs, ys)
                let failedPredicates = ref 0
                let ys = xs |> Stream.ofArray |> Stream.groupUntil false (fun i -> if i % 7 <> 0 then true else incr failedPredicates ; false) |> Stream.toSeq |> Seq.concat |> Seq.length
                Assert.AreEqual(xs.Length, ys + !failedPredicates)
            ).QuickCheckThrowOnFailure()


        [<Test>]
        member __.``seq/dispose``() =
            let disposed = ref false
            let testEnumerator (array : int []) = 
                disposed := false
                let index = ref -1
                {   new IEnumerator<int> with
                        member self.Current = array.[!index]
                    interface System.Collections.IEnumerator with  
                        member self.Current = box array.[!index]
                        member self.MoveNext() = 
                            incr index
                            if !index >= array.Length then false else true
                        member self.Reset() = index := -1 
                    interface System.IDisposable with  
                        member self.Dispose() = disposed := true }
            let testEnumerable (array : int []) = 
                {   new IEnumerable<int> with
                        member self.GetEnumerator() = testEnumerator array
                    interface System.Collections.IEnumerable with
                        member self.GetEnumerator() = testEnumerator array :> _ }
            Spec.ForAny<int []>(fun xs -> 
                let x = xs |> testEnumerable |> Stream.ofSeq |> Stream.filter (fun x -> x % 2 = 0) |> Stream.map ((+)1) |> Stream.toSeq |> Seq.length
                let y = xs |> Seq.filter (fun x -> x % 2 = 0) |> Seq.map ((+)1) |> Seq.length 
                x = y && !disposed = true).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``head``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x =
                    try xs |> Stream.ofArray |> Stream.head
                    with :? System.ArgumentException -> -1

                let y =
                    try xs |> Seq.head
                    with :? System.ArgumentException -> -1

                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``tryHead``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x = xs |> Stream.ofArray |> Stream.tryHead
                let y =
                    try Some (xs |> Seq.head)
                    with :? System.ArgumentException -> None

                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``isEmpty``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x = xs |> Stream.ofArray |> Stream.isEmpty
                let y = xs |> Array.isEmpty

                Assert.AreEqual(x, y)).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``reduce``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                if Array.isEmpty xs then
                    try let _ = xs |> Stream.ofArray |> Stream.reduce (+) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.reduce (+)
                    let y = xs |> Array.reduce (+)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``averageBy``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                if Array.isEmpty xs then
                    try let _ = xs |> Stream.ofArray |> Stream.averageBy (float) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.averageBy (float)
                    let y = xs |> Array.averageBy (float)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        member __.``average``() =
            Spec.ForAny<float []>(fun (xs : float []) ->
                if Array.isEmpty xs then
                    try let _ = xs |> Stream.ofArray |> Stream.average in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.average
                    let y = xs |> Array.average
                    x = y).QuickCheckThrowOnFailure()
