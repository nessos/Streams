namespace Nessos.Streams.Cloud

open Nessos.MBrace
open System
open System.Collections.Generic
open System.Runtime.Caching

[<Serializable; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CachedCloudArray<'T>(source : ICloudArray<'T>, taskId : string) = 
    let untyped = source :> ICloudArray
    
    member private this.StructuredFormatDisplay = source.ToString()
    member internal this.TaskId = taskId

    override this.ToString() = source.ToString()
    
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
type CloudArrayCache () =
    static let guid = System.Guid.NewGuid()

    // TODO : Replace
    static let createKey (ca : CachedCloudArray<'T>) start count = 
        sprintf "%s %s %d %d" (ca.ToString()) ca.TaskId start count
    static let parseKey (key : string) = 
        let key = key.Split()
        key.[0], key.[1], int64 key.[2], int key.[3]

    static let config = new System.Collections.Specialized.NameValueCollection()
    static do  config.Add("PhysicalMemoryLimitPercentage", "70")
    static let mc = new MemoryCache("CloudArrayMemoryCache", config)

    static let sync      = new obj()
    static let registry  = new HashSet<string * string * int64 * int>()
    static let occupied  = new HashSet<string>()
    static let policy    = new CacheItemPolicy()
    static do  policy.RemovedCallback <-
                new CacheEntryRemovedCallback(
                    fun args ->
                        lock sync (fun () -> registry.Remove(parseKey args.CacheItem.Key) |> ignore)
                )

    static member Guid = guid
    static member State = registry :> seq<_>
    static member Occupied = occupied :> seq<_>
        
    static member Add(ca : CachedCloudArray<'T>, start : int64, count : int, values : 'T []) =
        let key = createKey ca start count
        mc.Add(key, values, policy) |> ignore
        lock sync (fun () -> registry.Add((ca.ToString()), ca.TaskId, start, count) |> ignore)
  
    static member Get(ca : CachedCloudArray<'T>, parentTaskId  : string) : seq<int64 * int> =
        lock sync (fun () -> 
            if occupied.Contains parentTaskId then
                Seq.empty
            else
                occupied.Add(parentTaskId) |> ignore
                registry 
                |> Seq.filter (fun (key, tid, _, _) -> key = ca.ToString() && ca.TaskId = tid )
                |> Seq.map (fun (_,_,s,c) -> s,c))

    static member GetRange(ca : CachedCloudArray<'T>, start : int64, count : int) : 'T [] =
        let key = createKey ca start count
        mc.Get(key) :?> 'T []