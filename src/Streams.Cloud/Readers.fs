namespace Nessos.Streams.Cloud
    open System
    open System.IO

    [<AbstractClass; Sealed>]
    type CloudFile =
        static member ReadLines : Stream -> Async<string []> =
            fun stream -> 
                async { 
                    return [| use sr = new StreamReader(stream)
                              while not sr.EndOfStream do
                                  yield sr.ReadLine() |]
                }

        static member ReadText : Stream -> Async<string> =
            fun stream -> 
                async { 
                    use sr = new StreamReader(stream)
                    return sr.ReadToEnd()
                }

        static member ReadBytes : Stream -> Async<byte []> =
            fun stream -> 
                async {
                        use ms = new MemoryStream()
                        do! Async.AwaitTask(stream.CopyToAsync(ms).ContinueWith(ignore))
                        return ms.ToArray()
                }