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
                let x = xs |> Stream.ofArray |> Stream.filter (fun x -> x % 2 = 0) |> Stream.map ((+)1) |> Stream.toSeq |> Seq.length
                let y = xs |> Seq.filter (fun x -> x % 2 = 0) |> Seq.map ((+)1) |> Seq.length 
                x = y).QuickCheckThrowOnFailure()

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


