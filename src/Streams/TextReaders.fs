[<AutoOpen>]
module internal Nessos.Streams.TextReaders

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Text

// Implementation taken from MBrace.Core

/// Replacement implementation for System.IO.StreamReader which
/// will not expose number of bytes read in underlying stream.
/// This is used by RangedLineReader for proper partitioning of large text files.
type private StreamLineReader(stream : Stream, ?encoding : Encoding) = 
    let reader = match encoding with None -> new StreamReader(stream) | Some e -> new StreamReader(stream, e)
    let buffer : char [] = Array.zeroCreate 4096
    let mutable posInBuffer : int = -1
    let mutable numOfChars : int = 0
    let mutable endOfStream = false
    let mutable numberOfBytesRead = 0L
    let stringBuilder = new StringBuilder()
    /// Reads a line of characters from the current stream and returns the data as a string.
    member self.ReadLine() : string = 
        if endOfStream then 
            null
        else
            let mutable lineEndFlag = false
            while not lineEndFlag && not endOfStream do
                if posInBuffer = -1 then
                    posInBuffer <- 0
                    numOfChars <- reader.ReadBlock(buffer, posInBuffer, buffer.Length)
                    if numOfChars = 0 then
                        endOfStream <- true
                if not endOfStream then
                    let mutable i = posInBuffer 
                    while not lineEndFlag && i < numOfChars do
                        if buffer.[i] = '\n' then
                            stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                            lineEndFlag <- true
                            posInBuffer <- i + 1
                            numberOfBytesRead <- numberOfBytesRead + 1L
                        elif buffer.[i] = '\r' then
                            if i + 1 < numOfChars then
                                if buffer.[i + 1] = '\n' then
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- i + 2
                                    numberOfBytesRead <- numberOfBytesRead + 2L
                                else
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- i + 1
                                    numberOfBytesRead <- numberOfBytesRead + 1L
                            else 
                                let currentChar = char <| reader.Peek()
                                if currentChar = '\n' then
                                    reader.Read() |> ignore 
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- -1
                                    numberOfBytesRead <- numberOfBytesRead + 2L
                                else
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- -1
                                    numberOfBytesRead <- numberOfBytesRead + 1L
                        i <- i + 1
                
                    if not lineEndFlag then
                        stringBuilder.Append(buffer, posInBuffer, numOfChars - posInBuffer) |> ignore
                    if i = numOfChars then
                        posInBuffer <- -1
            
            let result = stringBuilder.ToString()
            stringBuilder.Clear() |> ignore
            numberOfBytesRead <- numberOfBytesRead + (int64 <| reader.CurrentEncoding.GetByteCount(result))
            result 

    /// The total number of bytes read
    member self.BytesRead = numberOfBytesRead

type private StreamLineEnumerator(stream : Stream, disposeStream : bool, ?encoding : Encoding) =
    let mutable currentLine = Unchecked.defaultof<string>
    let reader = 
        match encoding with 
        | None -> new StreamReader(stream) 
        | Some e -> new StreamReader(stream, e)

    interface IEnumerator<string> with
        member __.Current = currentLine
        member __.Current = box currentLine
        member __.MoveNext () =
            match reader.ReadLine () with
            | null -> false
            | line -> currentLine <- line ; true

        member __.Dispose () = if disposeStream then stream.Dispose()
        member __.Reset () = raise <| new NotSupportedException("LineReader")

type private RangedStreamLineEnumerator (stream : Stream, disposeStream : bool, beginPos : int64, endPos : int64, ?encoding : Encoding) =
    let mutable numberOfLines = 0L
    let mutable currentLine = Unchecked.defaultof<string>
    let mutable includeFirstLine = false
    do 
        if beginPos > endPos || endPos > stream.Length then raise <| new ArgumentOutOfRangeException("endPos")
        // include first line if:
        //   1. is the first line of the starting segment of a stream.
        //   2. is any successive line that fits within the stream boundary.
        if beginPos <> 0L then
            stream.Seek(beginPos - 1L, SeekOrigin.Begin) |> ignore
            if stream.ReadByte() = int '\n' then
                includeFirstLine <- true
        else 
            includeFirstLine <- true


    let reader = new StreamLineReader(stream, ?encoding = encoding)

    let rec readNext () : bool =
        let bytesRead = reader.BytesRead
        if beginPos + bytesRead <= endPos then
            let line = reader.ReadLine()
            if line = null then
                false
            else
                numberOfLines <- numberOfLines + 1L
                if numberOfLines = 1L && not includeFirstLine then
                    readNext()
                else
                    currentLine <- line
                    true
        else
            false
        

    interface IEnumerator<string> with
        member __.Current = currentLine
        member __.Current = box currentLine
        member __.MoveNext () = readNext ()
        member __.Dispose () = if disposeStream then stream.Dispose()
        member __.Reset () = raise <| new NotSupportedException("StreamLineReader")

type private StreamLineEnumerable(stream : Stream, disposeStream, ?encoding : Encoding) =
    interface IEnumerable<string> with
        member __.GetEnumerator() = new StreamLineEnumerator(stream, disposeStream, ?encoding = encoding) :> IEnumerator<string>
        member __.GetEnumerator() = new StreamLineEnumerator(stream, disposeStream, ?encoding = encoding) :> IEnumerator

/// Provides an enumerable implementation that reads text lines within the supplied seek range.
type private RangedStreamLineEnumerable(stream : Stream, disposeStream, beginPos : int64, endPos : int64, ?encoding : Encoding) =
    interface IEnumerable<string> with
        member __.GetEnumerator() = new RangedStreamLineEnumerator(stream, disposeStream, beginPos, endPos, ?encoding = encoding) :> IEnumerator<string>
        member __.GetEnumerator() = new RangedStreamLineEnumerator(stream, disposeStream, beginPos, endPos, ?encoding = encoding) :> IEnumerator

type TextReaders =

    /// <summary>
    ///     Reads all text from provided given stream with provided encoding.
    /// </summary>
    /// <param name="stream">Reader stream.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member ReadAllText (stream : Stream, ?encoding : Encoding) : string =
        let reader =
            match encoding with
            | None -> new StreamReader(stream)
            | Some e -> new StreamReader(stream, e)

        reader.ReadToEnd()
    
    /// <summary>
    ///     Returns an enumeration of all text lines contained in stream.
    /// </summary>
    /// <param name="stream">Reader stream.</param>
    /// <param name="disposeStream">Dispose stream on enumerator completion. Defaults to true.</param>
    /// <param name="encoding">Optional encoding for stream.</param>
    static member ReadLines (stream : Stream, ?disposeStream : bool, ?encoding : Encoding) : seq<string> =
        let disposeStream = defaultArg disposeStream true
        new StreamLineEnumerable(stream, disposeStream, ?encoding = encoding) :> _

    /// <summary>
    ///     Returns an enumeration of all text lines contained in stream within a supplied byte range.
    /// </summary>
    /// <param name="stream">Reader stream.</param>
    /// <param name="beginPos">Start position for stream.</param>
    /// <param name="endPos">End partition for stream.</param>
    /// <param name="disposeStream">Dispose stream on enumerator completion. Defaults to true.</param>
    /// <param name="encoding">Optional encoding for stream.</param>
    static member ReadLinesRanged (stream : Stream, beginPos : int64, endPos : int64, ?disposeStream : bool, ?encoding : Encoding) : seq<string> =
        let disposeStream = defaultArg disposeStream true
        new RangedStreamLineEnumerable(stream, disposeStream, beginPos, endPos, ?encoding = encoding) :> _