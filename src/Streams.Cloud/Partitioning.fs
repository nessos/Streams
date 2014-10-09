namespace Nessos.Streams.Cloud

module internal Partitions =
    open System
    open System.Collections.Generic
    open System.Collections.Concurrent

    let ofLongRange (n : int) (s : int64) (e : int64) : (int64 * int64) []  = 
        if n < 0 then invalidArg "n" "Must be greater than zero"
        if s > e then invalidArg "e" "Must be greater than s"

        let step = min ((e - s) / int64 n) (int64 Int32.MaxValue)

        let ranges = new ResizeArray<int64 * int64>(n)
        let mutable current = s
        while current + step <= e do
            ranges.Add(current, current + step)
            current <- current + step + 1L
        if current <= e then ranges.Add(current,e)
        ranges.ToArray()

    let ofRange (totalWorkers : int) (s : int) (e : int) : (int * int) [] = 
        ofLongRange totalWorkers (int64 s) (int64 e)
        |> Array.map (fun (s,e) -> int s, int e)

    let ofArray (totalWorkers : int) (array : 'T []) : 'T [] [] =
        let ranges = ofRange totalWorkers 0 array.Length
        let partitions = Array.zeroCreate ranges.Length
        ranges |> Array.iteri (fun i (s,e) -> partitions.[i] <- Array.sub array s (e-s))
        partitions