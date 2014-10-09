namespace Nessos.Streams.Cloud

module internal Partitions =
    open System
    open System.Collections.Generic
    open System.Collections.Concurrent

    let ofLongRange (n : int) (length : int64) : (int64 * int64) []  = 
        let n = int64 n
        [| 
            for i in 0L .. n - 1L ->
                let i, j = length * i / n, length * (i + 1L) / n in (i, j) 
        |]

    let ofRange (totalWorkers : int) (length : int) : (int * int) [] = 
        ofLongRange totalWorkers (int64 length)
        |> Array.map (fun (s,e) -> int s, int e)

    let ofArray (totalWorkers : int) (array : 'T []) : 'T [] [] =
        let ranges = ofRange totalWorkers array.Length
        let partitions = Array.zeroCreate ranges.Length
        ranges |> Array.iteri (fun i (s,e) -> partitions.[i] <- Array.sub array s (e-s))
        partitions