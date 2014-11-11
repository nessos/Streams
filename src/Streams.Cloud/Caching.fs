namespace Nessos.Streams.Cloud

open Nessos.MBrace
open System
open System.Collections.Generic
open System.Runtime.Caching
open System.Runtime.Serialization

[<Serializable; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type internal CachedCloudArray<'T>(source : ICloudArray<'T>, taskId : string) = 
    let untyped = source :> ICloudArray
    
    member private this.StructuredFormatDisplay = source.ToString()
    member internal this.TaskId = taskId

    override this.ToString() = source.ToString()
    
    interface ICloudArray<'T> with
        member this.Container = source.Container
        member this.Name = source.Name
        member this.Length = source.Length
        member this.Type = source.Type
        member this.Partitions = source.Partitions
        member this.GetPartition(index : int) = source.GetPartition(index)
        member this.Append(cloudArray : ICloudArray<'T>) = source.Append(cloudArray)
        member this.Item 
            with get (index : int64) = source.[index]
        member this.GetEnumerator() = source.GetEnumerator()
        member this.GetEnumerator() = untyped.GetEnumerator()
        member this.Dispose() = source.Dispose()

    interface ISerializable with
        member this.GetObjectData(info : SerializationInfo, context : StreamingContext) =
            info.AddValue("taskId" , taskId, typeof<string>)
            info.AddValue("source" , source, typeof<ICloudArray<'T>>)

    internal new(info : SerializationInfo, context : StreamingContext) =
        let taskId    = info.GetValue("taskId", typeof<string>) :?> string
        let source    = info.GetValue("source", typeof<ICloudArray<'T>>) :?> ICloudArray<'T>
        new CachedCloudArray<'T>(source, taskId)

[<Sealed;AbstractClass>]
type internal CloudArrayCache () =
    static let guid = System.Guid.NewGuid()

    // TODO : Replace
    static let createKey (ca : CachedCloudArray<'T>) pid = 
        sprintf "%s %s %d" (ca.ToString()) ca.TaskId pid
    static let parseKey (key : string) = 
        let key = key.Split()
        key.[0], key.[1], int key.[2]

    static let config = new System.Collections.Specialized.NameValueCollection()
    static do  config.Add("PhysicalMemoryLimitPercentage", "70")
    static let mc = new MemoryCache("CloudArrayMemoryCache", config)

    static let sync      = new obj()
    static let registry  = new HashSet<string * string * int>()
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
        
    static member Add(ca : CachedCloudArray<'T>, pid : int, values : 'T []) =
        let key = createKey ca pid
        mc.Add(key, values, policy) |> ignore
        lock sync (fun () -> registry.Add((ca.ToString()), ca.TaskId, pid) |> ignore)
  
    static member Get(ca : CachedCloudArray<'T>, parentTaskId  : string) : seq<int> =
        lock sync (fun () -> 
            if occupied.Contains parentTaskId then
                Seq.empty
            else
                occupied.Add(parentTaskId) |> ignore
                registry 
                |> Seq.filter (fun (key, tid, _) -> key = ca.ToString() && ca.TaskId = tid )
                |> Seq.map (fun (_,_,s) -> s))

    static member GetPartition(ca : CachedCloudArray<'T>, pid : int) : 'T [] =
        let key = createKey ca pid
        mc.Get(key) :?> 'T []