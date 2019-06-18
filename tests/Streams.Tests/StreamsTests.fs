#if INTERACTIVE
#r @"../../bin/Streams.Core.dll"
//#r @"../../packages/Streams.0.3.0/lib/net45/Streams.Core.dll"
#r @"../../packages/NUnit.3.0.0-alpha/lib/net45/nunit.framework.dll"
#r @"../../packages/FsCheck.1.0.1/lib/net45/FSCheck.dll"
#time "on"
#else
namespace Nessos.Streams.Tests
#endif
open System.Linq
open System.Collections.Generic
open FsCheck
open NUnit.Framework
open Nessos.Streams


type Spec =
    static member ForAny<'T> (prop : 'T -> bool) = Prop.forAll Arb.from<'T> prop
    static member ForAny<'T> (prop : 'T -> unit) = Prop.forAll Arb.from<'T> prop

[<TestFixture; Category("Streams.FSharp")>]
module ``Streams tests``  =

        [<Test>]
        let ``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map ((+)1) |> Stream.toArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``ofResizeArray/toResizeArray`` () =
            Spec.ForAny<ResizeArray<int>>(fun xs ->
                let x = xs |> Stream.ofResizeArray |> Stream.map ((+)1) |> Stream.toResizeArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                (x.ToArray()) = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``ofList`` () =
            Spec.ForAny<int list>(fun xs ->
                let x = xs |> Stream.ofList |> Stream.map ((+)1) |> Stream.toArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``ofList pull`` () =
            Spec.ForAny<int list>(fun xs ->
                let x = xs |> Stream.ofList |> Stream.map ((+)1) |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``ofSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofSeq |> Stream.map ((+)1) |> Stream.toArray
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``ofTextFileByLine`` () =
            let path = System.IO.Path.GetTempFileName()
            try
                let seed = seq { 1L .. 1000000L }
                System.IO.File.WriteAllLines(path, seed |> Seq.map (sprintf "This is file entry #%d"))

                let result =
                    Stream.ofTextFileByLine path
                    |> Stream.map (fun line -> line.Split('#').[1])
                    |> Stream.map int64
                    |> Stream.sum

                Assert.AreEqual(Seq.sum seed, result)

            finally System.IO.File.Delete(path)



        [<Test>]
        let ``generateInfinite`` () =
            Spec.ForAny<int>(fun n ->
                // we use modulus here to keep the test duration somewhat short.
                let x = (fun _ -> 1) |> Stream.generateInfinite |> Stream.take (abs(n) % 100) |> Stream.toArray
                let y = (fun _ -> 1) |> Seq.initInfinite |> Seq.take (abs(n) % 100) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``initInfinite`` () =
            Spec.ForAny<int>(fun n ->
                // we use modulus here to keep the test duration somewhat short.
                let x = (fun _ -> 1) |> Stream.initInfinite |> Stream.take (abs(n) % 100) |> Stream.toArray
                let y = (fun _ -> 1) |> Seq.initInfinite |> Seq.take (abs(n) % 100) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``toSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.flatMap (fun _ -> Stream.ofArray xs) |> Stream.filter (fun x -> x % 2 = 0) |> Stream.map ((+)1) |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.collect (fun _ -> xs) |> Seq.filter (fun x -> x % 2 = 0) |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``flatMap/toSeq`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.flatMap (fun x -> [|1..10|] |> Stream.ofArray) |> Stream.map ((+)1) |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.collect (fun x -> [|1..10|]) |> Seq.map ((+)1) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``flatMap/find`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = Seq.initInfinite id |> Stream.ofSeq |> Stream.flatMap (fun x -> Seq.initInfinite id |> Stream.ofSeq) |> Stream.map ((+)1) |> Stream.find (fun _ -> true)
                let y = Seq.initInfinite id |> Seq.collect (fun x -> Seq.initInfinite id) |> Seq.map ((+)1) |> Seq.find (fun _ -> true)
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``flatMap/take`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = Seq.initInfinite id |> Stream.ofSeq |> Stream.flatMap (fun x -> Seq.initInfinite id |> Stream.ofSeq |> Stream.take 10) |> Stream.map ((+)1) |> Stream.take 100 |> Stream.toArray
                let y = Seq.initInfinite id |> Seq.collect (fun x -> Seq.initInfinite id |> Seq.take 10) |> Seq.map ((+)1) |> Seq.take 100 |> Seq.toArray 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.toArray
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``mapi`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.mapi (fun i n -> i * n) |> Stream.toArray
                let y = xs |> Seq.mapi (fun i n -> i * n) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun n -> n % 2 = 0) |> Stream.toArray
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``choose`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.choose (fun n -> if n % 2 = 0 then Some n else None) |> Stream.toArray
                let y = xs |> Seq.choose (fun n -> if n % 2 = 0 then Some n else None) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``collect`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> Stream.toArray
                let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``fold`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.fold (+) 0 
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``scan`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.scan (+) 0 |> Stream.toArray 
                let y = xs |> Seq.scan (+) 0 |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()            

        [<Test>]
        let ``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map (fun n -> 2 * n) |> Stream.sum
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun n -> n % 2 = 0) |> Stream.length
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.map ((+) 1) |> Stream.sortBy id |> Stream.toArray
                let y = xs |> Seq.map ((+) 1) |> Seq.sortBy id |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``minBy`` () =
            Spec.ForAny<int[]>(fun xs -> 
                if Array.isEmpty xs then
                    try let _ = xs |> Stream.ofArray |> Stream.minBy (fun i -> i + 1) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.minBy (fun i -> i + 1)
                    let y = xs |> Seq.minBy (fun i -> i + 1)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``maxBy`` () =
            Spec.ForAny<int[]>(fun xs -> 
                if Array.isEmpty xs then 
                    try let _ = xs |> Stream.ofArray |> Stream.maxBy (fun i -> i + 1) in false
                    with :? System.ArgumentException -> true
                else
                    let x = xs |> Stream.ofArray |> Stream.maxBy (fun i -> i + 1)
                    let y = xs |> Seq.maxBy (fun i -> i + 1)
                    x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``countBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.countBy (fun i -> i % 5) |> Stream.toArray
                let y = xs |> Seq.countBy (fun i -> i % 5) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``foldBy`` () =
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
        let ``groupBy`` () =
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
        let ``take`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) ->
                let n = System.Math.Abs(n) 
                let x = xs |> Stream.ofArray |> Stream.take n |> Stream.length
                let y = xs.Take(n).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``take/boundary check`` () =
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
        let ``takeWhile`` () =
            Spec.ForAny<int[]>(fun xs ->
                let pred = (fun value -> value % 2 = 0)
                let x = xs |> Stream.ofArray |> Stream.takeWhile pred |> Stream.length
                let y = xs.TakeWhile(pred).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``collect/take`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) ->
                let n = System.Math.Abs(n) 
                let x = xs |> Stream.ofArray |> Stream.collect(fun x -> xs |> Stream.ofArray |> Stream.take n) |> Stream.length
                let y = xs.SelectMany(fun x -> xs.Take(n)).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``skip`` () =
            Spec.ForAny<int[] * int>(fun (xs, (n : int)) -> 
                let x = xs |> Stream.ofArray |> Stream.skip n |> Stream.length
                let y = xs.Skip(n).Count()
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``tryFind`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.tryFind (fun n -> n % 2 = 0) 
                let y = xs |> Seq.tryFind (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``find`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = try xs |> Stream.ofArray |> Stream.find (fun n -> n % 2 = 0) with | :? KeyNotFoundException -> -1
                let y = try xs |> Seq.find (fun n -> n % 2 = 0) with | :? KeyNotFoundException -> -1
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``tryPick`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.tryPick (fun n -> if n % 2 = 0 then Some n else None) 
                let y = xs |> Seq.tryPick (fun n -> if n % 2 = 0 then Some n else None) 
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``pick`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = try xs |> Stream.ofArray |> Stream.pick (fun n -> if n % 2 = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                let y = try xs |> Seq.pick (fun n -> if n % 2 = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``exists`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.exists (fun n -> n % 2 = 0) 
                let y = xs |> Seq.exists (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()


        [<Test>]
        let ``forall`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> Stream.ofArray |> Stream.forall (fun n -> n % 2 = 0) 
                let y = xs |> Seq.forall (fun n -> n % 2 = 0) 
                x = y).QuickCheckThrowOnFailure()



        [<Test>]
        let ``zipWith`` () =
            Spec.ForAny<(int[] * int[])>(fun (xs, ys) ->
                let x = xs |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0) |> Stream.zipWith (fun x y -> x + y) (ys |> Stream.ofArray) |> Stream.toArray
                let y = xs |> Seq.filter (fun x -> x % 2 = 0) |> Seq.zip ys |> Seq.map (fun (x, y) -> x + y) |> Seq.toArray
                x = y).QuickCheckThrowOnFailure()

        [<Test>]
        let ``empty`` () =
            //for bulk
            Assert.AreEqual(0, Stream.empty<int> |> Stream.length)
            //for next
            Assert.AreEqual(0, Stream.empty<int> |> Stream.toSeq |> Seq.length)

        [<Test>]
        let ``concat`` () =
            Spec.ForAny<int[][]>(fun xs ->
                let x = xs |> Seq.map (fun xs' -> xs' |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0)) |> Stream.concat |> Stream.toArray
                let y = xs |> Seq.map (fun xs' -> xs' |> Seq.filter (fun x -> x % 2 = 0)) |> Seq.concat |> Seq.toArray
                Assert.AreEqual(x, y)
                let x = xs |> Seq.map (fun xs' -> xs' |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0)) |> Stream.concat |> Stream.toSeq |> Seq.toArray
                let y = xs |> Seq.map (fun xs' -> xs' |> Seq.filter (fun x -> x % 2 = 0)) |> Seq.concat |> Seq.toArray
                Assert.AreEqual(x, y)
                ).QuickCheckThrowOnFailure()


        [<Test>]
        let ``cast`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |>  Stream.cast |> Stream.toArray
                let y = xs |> Seq.cast |> Seq.toArray
                Assert.AreEqual(x, y)
                let x = xs |>  Stream.cast |> Stream.toSeq |> Seq.length
                let y = xs |> Seq.cast |> Seq.length
                Assert.AreEqual(x, y)
                ).QuickCheckThrowOnFailure()

        [<Test>]
        let ``cache``() =
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
        let ``groupUntil``() =
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
        let ``seq/dispose``() =
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
        let ``head``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x =
                    try xs |> Stream.ofArray |> Stream.head
                    with :? System.ArgumentException -> -1

                let y =
                    try xs |> Seq.head
                    with :? System.ArgumentException -> -1

                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        let ``tryHead``() =
            Spec.ForAny<int []>(fun (xs : int []) ->
                let x = xs |> Stream.ofArray |> Stream.tryHead
                let y =
                    try Some (xs |> Seq.head)
                    with :? System.ArgumentException -> None

                Assert.AreEqual(y, x)).QuickCheckThrowOnFailure()

        [<Test>]
        let ``isEmpty``() =
            Spec.ForAny<int []>(fun (xs : int  []) ->
                let x = xs |> Stream.ofArray |> Stream.isEmpty
                let y = xs |> Array.isEmpty

                Assert.AreEqual(x, y)).QuickCheckThrowOnFailure()

module PerfTests = 
  let AdhocPerformanceTest() = 

    let xs = Array.init 1000000 id

    let runSeq() =
        xs
        |> Seq.map ((+) 1)
        |> Seq.map ((-) 1)
        |> Seq.map ((+) 1)
        |> Seq.map ((-) 1)
        |> Seq.map ((+) 1)
        |> Seq.map ((-) 1)
        |> Seq.map ((+) 1)
        |> Seq.map ((-) 1)
        |> Seq.map ((+) 1)
        |> Seq.map ((-) 1)
        |> Seq.filter (fun x -> x % 2 = 0)
        |> Seq.length

    let runList() =
        xs
        |> List.ofArray
        |> List.map ((+) 1)
        |> List.map ((-) 1)
        |> List.map ((+) 1)
        |> List.map ((-) 1)
        |> List.map ((+) 1)
        |> List.map ((-) 1)
        |> List.map ((+) 1)
        |> List.map ((-) 1)
        |> List.map ((+) 1)
        |> List.map ((-) 1)
        |> List.filter (fun x -> x % 2 = 0)
        |> List.length

 
    let runArray() =
        xs
        |> Array.map ((+) 1)
        |> Array.map ((-) 1)
        |> Array.map ((+) 1)
        |> Array.map ((-) 1)
        |> Array.map ((+) 1)
        |> Array.map ((-) 1)
        |> Array.map ((+) 1)
        |> Array.map ((-) 1)
        |> Array.map ((+) 1)
        |> Array.map ((-) 1)
        |> Array.filter (fun x -> x % 2 = 0)
        |> Array.length

    let runStream n =
     for i in 1 .. n do
        xs 
        |> Stream.ofArray
        |> Stream.take 1000000
        |> Stream.map ((+) 1)
        |> Stream.map ((-) 1)
        |> Stream.map ((+) 1)
        |> Stream.map ((-) 1)
        |> Stream.map ((+) 1)
        |> Stream.map ((-) 1)
        |> Stream.map ((+) 1)
        |> Stream.map ((-) 1)
        |> Stream.map ((+) 1)
        |> Stream.map ((-) 1)
        |> Stream.filter (fun x -> x % 2 = 0)
        |> Stream.length
        |> ignore

    runSeq() |> ignore
    runList() |> ignore
    runStream 100 |> ignore
      //0.3.0: 
      //   Real: 00:00:03.352, CPU: 00:00:03.343, GC gen0: 0, gen1: 0, gen2: 0
      //   Real: 00:00:02.631, CPU: 00:00:02.640, GC gen0: 0, gen1: 0, gen2: 0
      //   Real: 00:00:02.858, CPU: 00:00:02.843, GC gen0: 0, gen1: 0, gen2: 0
      //   Real: 00:00:03.156, CPU: 00:00:03.156, GC gen0: 0, gen1: 0, gen2: 0
      //
      //New (fully hidden represenations, without any inlining)
      //   Real: 00:00:17.211, CPU: 00:00:16.937, GC gen0: 1, gen1: 0, gen2: 0
      //   Real: 00:00:17.750, CPU: 00:00:17.687, GC gen0: 0, gen1: 0, gen2: 0
      //
      //New (fully hidden represenations, with inlining reducing to Internals)
      //   Real: 00:00:02.384, CPU: 00:00:02.375, GC gen0: 0, gen1: 0, gen2: 0
      //   Real: 00:00:02.490, CPU: 00:00:02.484, GC gen0: 0, gen1: 0, gen2: 0
      //   Real: 00:00:02.362, CPU: 00:00:02.343, GC gen0: 0, gen1: 0, gen2: 0



    runArray() |> ignore
 

