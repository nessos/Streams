namespace Nessos.Streams.Internals
open System
open System.Threading.Tasks
open System.Threading

type MergeArrayType = FromArrayType | ToArrayType 

/// [omit]
/// Helpers for parallel sorting.
module Sort = 
    open System.Collections.Generic

    // TODO: Remove code duplication

    let inline parallelSort<'Key, 'Value when 'Key : comparison>
        (totalWorkers : int) (descending : bool) (keys : 'Key[]) (array : 'Value[]) = 
            // Taken from Carl Nolan's parallel inplace merge
            // The merge of the two array
            let merge (toArray: 'Value []) (toKeys : 'Key[]) (fromArray: 'Value []) (fromKeys : 'Key[]) (low1: int) (low2: int) (high1: int) (high2: int) =
                let mutable ptr1 = low1
                let mutable ptr2 = high1
 
                for ptr = low1 to high2 do
                    if (ptr1 > low2) then
                        toArray.[ptr] <- fromArray.[ptr2]
                        toKeys.[ptr] <- fromKeys.[ptr2]
                        ptr2 <- ptr2 + 1
                    elif (ptr2 > high2) then
                        toArray.[ptr] <- fromArray.[ptr1]
                        toKeys.[ptr] <- fromKeys.[ptr1]
                        ptr1 <- ptr1 + 1
                    else
                        if descending then
                            if  fromKeys.[ptr1] > fromKeys.[ptr2] then
                                toArray.[ptr] <- fromArray.[ptr1]
                                toKeys.[ptr] <- fromKeys.[ptr1]
                                ptr1 <- ptr1 + 1             
                            else
                                toArray.[ptr] <- fromArray.[ptr2]
                                toKeys.[ptr] <- fromKeys.[ptr2]
                                ptr2 <- ptr2 + 1
                        else
                            if  fromKeys.[ptr1] <= fromKeys.[ptr2] then
                                toArray.[ptr] <- fromArray.[ptr1]
                                toKeys.[ptr] <- fromKeys.[ptr1]
                                ptr1 <- ptr1 + 1
                            else
                                toArray.[ptr] <- fromArray.[ptr2]
                                toKeys.[ptr] <- fromKeys.[ptr2]
                                ptr2 <- ptr2 + 1

 
            // define the sort operation
            let parallelSort () =
 
                // control flow parameters
                let auxArray : 'Value array = Array.zeroCreate array.Length
                let auxKeys : 'Key array = Array.zeroCreate array.Length
                let workers : Task array = Array.zeroCreate (totalWorkers - 1)
                let iterations = int (Math.Log((float totalWorkers), 2.0))
 

 
                // Number of elements for each array, if the elements number is not divisible by the workers
                // the remainders will be added to the first worker (the main thread)
                let partitionSize = ref (int (array.Length / totalWorkers))
                let remainder = array.Length % totalWorkers
 
                // Define the arrays references for processing as they are swapped during each iteration
                let swapped = ref false
 
                let inline getMergeArray (arrayType: MergeArrayType) =
                    match (arrayType, !swapped) with
                    | (FromArrayType, true) -> (auxArray, auxKeys)
                    | (FromArrayType, false) -> (array, keys)
                    | (ToArrayType, true) -> (array, keys)
                    | (ToArrayType, false) -> (auxArray, auxKeys)
 
                use barrier = new Barrier(totalWorkers, fun (b) ->
                    partitionSize := !partitionSize <<< 1
                    swapped := not !swapped)
 
                // action to perform the sort an merge steps
                let action (index: int) =   
                         
                    //calculate the partition boundary
                    let low = index * !partitionSize + match index with | 0 -> 0 | _ -> remainder
                    let high = (index + 1) * !partitionSize - 1 + remainder
 
                    // Sort the specified range - could implement QuickSort here
                    let sortLen = high - low + 1
                    Array.Sort(keys, array, low, sortLen)
                    if descending then
                        Array.Reverse(keys, low, sortLen)
                        Array.Reverse(array, low, sortLen)
 
                    barrier.SignalAndWait()
 
                    let rec loopArray loopIdx actionIdx loopHigh =
                        if loopIdx < iterations then
                            if (actionIdx % 2 = 1) then
                                barrier.RemoveParticipant()
                            else
                                let newHigh = loopHigh + !partitionSize / 2
                                let toArray, toKeys = getMergeArray FromArrayType
                                let fromArray, fromKeys = getMergeArray ToArrayType
                                merge toArray toKeys fromArray fromKeys low loopHigh (loopHigh + 1) newHigh
                                barrier.SignalAndWait()
                                loopArray (loopIdx + 1) (actionIdx >>> 1) newHigh
                    loopArray 0 index high
 
                for index in 1 .. workers.Length do
                    workers.[index - 1] <- Task.Factory.StartNew(fun() -> action index)
 
                action 0
 
                // if odd iterations return auxArray otherwise array (swapped will be false)
                if not (iterations % 2 = 0) then  
                    Array.blit auxArray 0 array 0 array.Length
                    Array.blit auxKeys 0 keys 0 keys.Length
 
            parallelSort()


    let inline parallelSortWithComparer<'Key, 'Value>
        (totalWorkers : int) (comparer : IComparer<'Key>) (keys : 'Key[]) (array : 'Value[]) = 
            // Taken from Carl Nolan's parallel inplace merge
            // The merge of the two array
            let merge (toArray: 'Value []) (toKeys : 'Key[]) (fromArray: 'Value []) (fromKeys : 'Key[]) (low1: int) (low2: int) (high1: int) (high2: int) =
                let mutable ptr1 = low1
                let mutable ptr2 = high1
 
                for ptr = low1 to high2 do
                    if (ptr1 > low2) then
                        toArray.[ptr] <- fromArray.[ptr2]
                        toKeys.[ptr] <- fromKeys.[ptr2]
                        ptr2 <- ptr2 + 1
                    elif (ptr2 > high2) then
                        toArray.[ptr] <- fromArray.[ptr1]
                        toKeys.[ptr] <- fromKeys.[ptr1]
                        ptr1 <- ptr1 + 1
                    elif  comparer.Compare(fromKeys.[ptr1], fromKeys.[ptr2]) <= 0 then
                        toArray.[ptr] <- fromArray.[ptr1]
                        toKeys.[ptr] <- fromKeys.[ptr1]
                        ptr1 <- ptr1 + 1
                    else
                        toArray.[ptr] <- fromArray.[ptr2]
                        toKeys.[ptr] <- fromKeys.[ptr2]
                        ptr2 <- ptr2 + 1

 
            // define the sort operation
            let parallelSort () =
 
                // control flow parameters
                let auxArray : 'Value array = Array.zeroCreate array.Length
                let auxKeys : 'Key array = Array.zeroCreate array.Length
                let workers : Task array = Array.zeroCreate (totalWorkers - 1)
                let iterations = int (Math.Log((float totalWorkers), 2.0))
 

 
                // Number of elements for each array, if the elements number is not divisible by the workers
                // the remainders will be added to the first worker (the main thread)
                let partitionSize = ref (int (array.Length / totalWorkers))
                let remainder = array.Length % totalWorkers
 
                // Define the arrays references for processing as they are swapped during each iteration
                let swapped = ref false
 
                let inline getMergeArray (arrayType: MergeArrayType) =
                    match (arrayType, !swapped) with
                    | (FromArrayType, true) -> (auxArray, auxKeys)
                    | (FromArrayType, false) -> (array, keys)
                    | (ToArrayType, true) -> (array, keys)
                    | (ToArrayType, false) -> (auxArray, auxKeys)
 
                use barrier = new Barrier(totalWorkers, fun (b) ->
                    partitionSize := !partitionSize <<< 1
                    swapped := not !swapped)
 
                // action to perform the sort an merge steps
                let action (index: int) =   
                         
                    //calculate the partition boundary
                    let low = index * !partitionSize + match index with | 0 -> 0 | _ -> remainder
                    let high = (index + 1) * !partitionSize - 1 + remainder
 
                    // Sort the specified range - could implement QuickSort here
                    let sortLen = high - low + 1
                    Array.Sort(keys, array, low, sortLen, comparer)
 
                    barrier.SignalAndWait()
 
                    let rec loopArray loopIdx actionIdx loopHigh =
                        if loopIdx < iterations then
                            if (actionIdx % 2 = 1) then
                                barrier.RemoveParticipant()
                            else
                                let newHigh = loopHigh + !partitionSize / 2
                                let toArray, toKeys = getMergeArray FromArrayType
                                let fromArray, fromKeys = getMergeArray ToArrayType
                                merge toArray toKeys fromArray fromKeys low loopHigh (loopHigh + 1) newHigh
                                barrier.SignalAndWait()
                                loopArray (loopIdx + 1) (actionIdx >>> 1) newHigh
                    loopArray 0 index high
 
                for index in 1 .. workers.Length do
                    workers.[index - 1] <- Task.Factory.StartNew(fun() -> action index)
 
                action 0
 
                // if odd iterations return auxArray otherwise array (swapped will be false)
                if not (iterations % 2 = 0) then  
                    Array.blit auxArray 0 array 0 array.Length
                    Array.blit auxKeys 0 keys 0 keys.Length
 
            parallelSort()
