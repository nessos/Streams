namespace Nessos.Streams.Cloud
open System
open System.Collections.Generic
open System.Collections.Concurrent

/// StartIndex * Length.
type internal CloudArrayRange = int64 * int
/// CloudArray identifier.
type internal CloudArrayId    = string

[<Sealed;AbstractClass>]
type internal CloudArrayRangeRegistry () =
    static let guid     = System.Guid.NewGuid()
    static let registry = ConcurrentDictionary<CloudArrayId, List<CloudArrayRange>>()
        
    static member GetRegistryId () = guid
        
    static member Add(id : CloudArrayId, range : CloudArrayRange) =
        registry.AddOrUpdate(
            id, 
            new List<CloudArrayRange>([range]), 
            fun _ (existing : List<CloudArrayRange>) -> existing.Add(range); existing)
        |> ignore
        
    static member Get(id : CloudArrayId) =
        match registry.TryGetValue(id) with
        | true, ranges -> ranges
        | _ -> failwith "Non-existent CloudArray"

    static member GetNumberOfRanges(id : CloudArrayId) =
        CloudArrayRangeRegistry.Get(id).Count

    static member Contains(id : CloudArrayId) =
        registry.ContainsKey(id)