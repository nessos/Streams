namespace Nessos.Streams.Cloud
    open System
    open System.IO

    [<AbstractClass; Sealed>]
    type CloudFile =
        static member ReadLines : Stream -> Async<seq<string>> =
            fun stream -> 
                async { 
                    return seq {   
                        use sr = new StreamReader(stream)
                        while not sr.EndOfStream do
                            yield sr.ReadLine() }
                }

        static member ReadAllLines : Stream -> Async<string []> =
            fun stream -> 
                async { 
                    return [| use sr = new StreamReader(stream)
                              while not sr.EndOfStream do
                                  yield sr.ReadLine() |]
                }

        static member ReadAllText : Stream -> Async<string> =
            fun stream -> 
                async { 
                    use sr = new StreamReader(stream)
                    return sr.ReadToEnd()
                }

        static member ReadAllBytes : Stream -> Async<byte []> =
            fun stream -> 
                async {
                        use ms = new MemoryStream()
                        do! Async.AwaitTask(stream.CopyToAsync(ms).ContinueWith(ignore))
                        return ms.ToArray()
                }