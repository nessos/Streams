namespace Nessos.Streams.Cloud

module internal Partitions =
    open System.Collections.Generic
    open System.Collections.Concurrent

    let ofRange (totalWorkers : int) (s : int64) (e : int64) = 
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