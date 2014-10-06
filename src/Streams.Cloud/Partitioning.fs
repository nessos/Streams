namespace Nessos.Streams.Cloud

module internal Partitions =
    open System.Collections.Generic
    open System.Collections.Concurrent

    let ofLongRange (totalWorkers : int) (s : int64) (e : int64) = 
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

    let ofRange (totalWorkers : int) (s : int) (e : int) = 
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

    let ofArray (totalWorkers : int) (array : 'T []) : 'T [] [] =
        let ranges = ofRange totalWorkers 0 array.Length
        let partitions = Array.zeroCreate ranges.Length
        ranges |> Array.iteri (fun i (s,e) -> partitions.[i] <- Array.sub array s (e-s))
        partitions