namespace Nessos.Streams.Cloud

open Nessos.MBrace
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Runtime.Caching


(****************************************************************
 * TODO :                                                       *
 * 1. Remove Cache methods                                      *
 * 2. Merge and simplify CloudArrayRegistry and CloudArrayCache *
 * 3. No need for fancy Cache.FetchRange stuff.                 *
 * 4. Remove CloudArrayId and CloudArrayRange                   *
 * 5. Remove 'T [] and implement big arrays                     *
 ****************************************************************)

[<Serializable; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type internal CachedCloudArray<'T>(source : ICloudArray<'T>) = 
    let untyped = source :> ICloudArray
    
    member private this.StructuredFormatDisplay = source.ToString()
    
    interface ICloudArray<'T> with
        member this.Container = source.Container
        member this.Name = source.Name
        member this.Length = source.Length
        member this.Type = source.Type
        member this.Range(start : int64, count : int) = source.Range(start, count)
        member this.Append(cloudArray : ICloudArray<'T>) = source.Append(cloudArray)
        member this.Item 
            with get (index : int64) = source.[index]
        member this.GetEnumerator() = source.GetEnumerator()
        member this.Range(start : int64, count : int) = untyped.Range(start, count)
        member this.Append(cloudArray : ICloudArray) = untyped.Append(cloudArray)
        member this.Item 
            with get (index : int64) = untyped.[index]
        member this.GetEnumerator() = untyped.GetEnumerator()
        member this.Cache() : ICachedCloudArray = failwith "Invalid"
        member this.Cache() : ICachedCloudArray<'T> = failwith "Invalid"

[<Sealed;AbstractClass>]
type internal CloudArrayCache private () =
    static let guid = System.Guid.NewGuid()

    static let createKey ca start count = sprintf "%s %d %d" (ca.ToString()) start count
    static let parseKey(key : string) = let key = key.Split() in key.[0], int64 key.[1], int key.[2]

    static let config = new System.Collections.Specialized.NameValueCollection()
    static do  config.Add("PhysicalMemoryLimitPercentage", "70")
    static let mc = new MemoryCache("CloudArrayMemoryCache", config)

    static let sync      = new obj()
    static let registry  = new HashSet<string * int64 * int>()
    static let policy    = new CacheItemPolicy()
    static do  policy.RemovedCallback <-
                new CacheEntryRemovedCallback(
                    fun args ->
                        lock sync (fun () -> registry.Remove(parseKey args.CacheItem.Key) |> ignore)
                )

    static member Guid = guid
    static member State = registry :> seq<_>
        
    static member Add(ca : ICloudArray<'T>, start : int64, count : int, values : 'T []) =
        let key = createKey ca start count
        mc.Add(key, values, policy) |> ignore
        lock sync (fun () -> registry.Add((ca.ToString()),start, count) |> ignore)
  
    static member Get(ca : ICloudArray<'T>) : seq<int64 * int> =
        lock sync (fun () -> 
            registry 
            |> Seq.filter (fun (key,s, c) -> key = ca.ToString() )
            |> Seq.map (fun (_,s,c) -> s,c))

    static member Get(ca : ICloudArray<'T>, start : int64, count : int) : 'T [] =
        let key = createKey ca start count
        mc.Get(key) :?> 'T []