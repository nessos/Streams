#load "../../packages/MBrace.Runtime.0.5.13-alpha/bootstrap.fsx" 
#r "../../bin/Streams.Core.dll"
#r "../../bin/Streams.Cloud.dll"

open Nessos.Streams.Cloud
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

#time

let rnd = new System.Random()
let data = Array.init 100 id

let runtime = MBrace.InitLocal(totalNodes = 4, store = FileSystemStore.LocalTemp)
let run (cloud : Cloud<'T>) = 
    runtime.Run cloud 
    //MBrace.RunLocal cloud

open System.IO
let xs = [|null|] : string []
let cfs = 
    xs |> Array.map(fun text -> 
        StoreClient.Default.CreateCloudFile(System.Guid.NewGuid().ToString(),
            (fun (stream : Stream) -> 
                async {
                    use sw = new StreamWriter(stream)
                    sw.Write(text) })))


let rec partitionByLength (files : ICloudFile []) index (currLength : int64) (currAcc : ICloudFile list) (acc : ICloudFile list list)=
    async {
        let max = 1L
        if index >= files.Length then return (currAcc :: acc) |> List.filter (not << List.isEmpty)
        else
            use! stream = files.[index].Read()
            if stream.Length >= max then
                return! partitionByLength files (index + 1) stream.Length [files.[index]] (currAcc :: acc)
            elif stream.Length + currLength >= max then
                return! partitionByLength files index 0L [] (currAcc :: acc)
            else
                return! partitionByLength files (index + 1) (currLength + stream.Length) (files.[index] :: currAcc) acc
    }

let partitions = partitionByLength cfs 0 0L [] [] |> Async.RunSynchronously

let x = cfs |> CloudStream.ofCloudFiles CloudFile.ReadAllText
            |> CloudStream.toArray
            |> MBrace.RunLocal
            |> Set.ofArray

let y = cfs |> Array.map (fun cf -> cf.Read())
            |> Array.map (fun s -> async { let! s = s in return! CloudFile.ReadAllText s })
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Set.ofArray


open System.IO

let cfs = 
    xs |> Array.mapi(fun i text -> 
        StoreClient.Default.CreateCloudFile(string i + ".txt",
            (fun (stream : Stream) -> 
                async {
                    use sw = new StreamWriter(stream)
                    sw.Write(text) })))

let x = cfs |> CloudStream.ofCloudFiles CloudFile.ReadAllText
            |> CloudStream.toArray
            |> MBrace.RunLocal

let y = cfs |> Array.map (fun cf -> cf.Read())
            |> Array.map (fun s -> async { let! s = s in return! CloudFile.ReadAllText s })
            |> Async.Parallel
            |> Async.RunSynchronously



let ca' =
    cached
    |> CloudStream.ofCloudArray 
    |> CloudStream.map (fun x -> x * x)
    |> CloudStream.toCloudArray
    |> run

ca' |> Seq.toArray |> Seq.length


let path = @"C:\dev\github-repositories\MBrace.Demos\data\Shakespeare"

let cfs = runtime.GetStoreClient().UploadFiles(System.IO.Directory.GetFiles path)

open Nessos.Streams

let r = 
    cfs
    |> CloudStream.ofCloudFiles CloudFile.ReadLines
    |> CloudStream.collect (fun lines -> Stream.ofSeq lines)
    |> CloudStream.map id
    |> CloudStream.length
    |> runtime.Run

let cas = System.IO.Directory.GetFiles path
          |> Array.map (fun file -> let vs = System.IO.File.ReadLines(file) in runtime.GetStoreClient().CreateCloudArray("tmp", vs))
          |> Array.reduce (fun l r -> l.Append(r))

let r' = 
    cas
    |> CloudStream.ofCloudArray 
    |> CloudStream.map id
    |> CloudStream.length
    |> runtime.Run


let xs : string [] [] = [|[|null|]|]

open System.IO
open Nessos.Streams

let cfs = 
    xs |> Array.map(fun xs -> 
        StoreClient.Default.CreateCloudFile(System.Guid.NewGuid().ToString(),
            (fun (stream : Stream) -> 
                async {
                    use sw = new StreamWriter(stream)
                    xs |> Array.iter (sw.WriteLine) })))
cfs.[0].Size

let x = cfs |> CloudStream.ofCloudFiles CloudFile.ReadLines
            |> CloudStream.collect (fun s -> printfn "%A" s ; Stream.ofSeq s)
            |> CloudStream.toArray
            |> MBrace.RunLocal

let y = xs |> Array.collect id



open System.Collections.Generic
open System.Collections.Concurrent

let ofLongRange (totalWorkers : int) (s : int64) (e : int64) : (int64 * int64) []  = 
    let toSeq (enum : IEnumerator<_>)= 
        seq {
            while enum.MoveNext() do
                yield enum.Current
        }
    let partitioner = Partitioner.Create(s, e)
    let partitions = partitioner.GetPartitions(totalWorkers) 
                        |> Seq.collect toSeq 
                        |> Seq.toArray 
    partitions


ofLongRange 1 80L 1000000L |> Seq.length





let partition (s : int) (e : int) (n : int) =
    if n < 0 then invalidArg "n" "Must be greater than zero"
    if s > e then invalidArg "e" "Must be greater than s"

    let step = (e - s) / n
    let ranges = new ResizeArray<int * int>(n)
    let mutable current = s
    while current + step <= e do
        ranges.Add(current, current + step)
        current <- current + step + 1
    if current <= e then ranges.Add(current,e)
    ranges.ToArray()


#r @"C:\dev\github-repositories\Streams\packages\FsCheck.1.0.0\lib\net45\FsCheck.dll"

open FsCheck
open FsCheck.Fluent

Spec.ForAny<int*int*int>(fun (s,e,n) ->
        if s > e || n <= 0 then
            true
        else
            let ps = partition s e n
            ps.Length <= n 
    ).QuickCheckThrowOnFailure()

partition 0 0 2

//cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.State }) |> Cloud.Parallel }
//|> run
//|> Seq.iter (
//    fun state ->
//        printfn "-------------------------------------" 
//        state |> Seq.sort |> (Seq.iter (printfn "%A")))
//
//CloudArrayCache.State 
//|> Seq.sort
//|> Seq.iter (printfn "%A")
//
//cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Guid }) |> Cloud.Parallel }
//|> run
//
//
//cloud { let! n = Cloud.GetWorkerCount() in return! [|1..n|] |> Array.map (fun _ -> cloud { return CloudArrayCache.Occupied }) |> Cloud.Parallel }
//|> run
//
//let cloudArray = StoreClient.Default.CreateCloudArray<int>("temp", Seq.empty) 
//let cached = CloudStream.cache cloudArray |> run
//
//CloudArrayCache.Occupied