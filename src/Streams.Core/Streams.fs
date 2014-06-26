namespace Streams.Core

module Stream =
    
    type Stream<'T> = Stream of (('T -> unit) -> unit)
        
    // generator functions
    let inline ofArray (values : 'T []) : Stream<'T> =
        Stream (fun iterf -> 
                    for i = 0 to values.Length - 1 do
                        iterf values.[i])

    let inline ofSeq (values : seq<'T>) : Stream<'T> =
        Stream (fun iterf -> 
                    for value in values do
                        iterf value)

    // intermediate functions
    let inline map (f : 'T -> 'R) (stream : Stream<'T>) : Stream<'R> =
        Stream (fun iterf -> 
            let (Stream streamf) = stream 
            streamf (fun value -> iterf (f value)))

    let inline flatmap (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        Stream (fun iterf -> 
            let (Stream streamf) = stream 
            streamf (fun value -> 
                        let (Stream streamf') = f value 
                        streamf' iterf))

    let inline collect (f : 'T -> Stream<'R>) (stream : Stream<'T>) : Stream<'R> =
        flatmap f stream

    let inline filter (p : 'T -> bool) (stream : Stream<'T>) : Stream<'T> =
        Stream (fun iterf -> 
            let (Stream streamf) = stream 
            streamf (fun value -> if p value then iterf value))
        
    // terminal functions
    let inline reduce (reducef : 'T -> 'R -> 'R) (init : 'R) (stream : Stream<'T>) : 'R = 
        let accRef = ref init
        let (Stream streamf) = stream 
        streamf (fun value -> accRef := reducef value !accRef)
        !accRef

    let inline sum (stream : Stream<int64>) : int64 = 
        reduce (+) 0L stream
