using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.Streams.Cloud.CSharp
{
    internal class AsyncEnumerator : IEnumerator<string>
    {
        StreamReader reader;
        string current;

        public AsyncEnumerator(System.IO.Stream stream)
        {
            this.reader = new StreamReader(stream);
        }

        public string Current
        {
            get { return current; }
        }

        public void Dispose()
        {
            reader.Dispose();
        }

        object System.Collections.IEnumerator.Current
        {
            get { return current; }
        }

        public bool MoveNext()
        {
            if (reader.EndOfStream) return false;
            else
            {
                current = reader.ReadLine();
                return true;
            }
        }

        public void Reset()
        {
            reader.BaseStream.Seek(0L, SeekOrigin.Begin);
        }

    }

    internal class AsyncEnumerable : IEnumerable<string>
    {
        System.IO.Stream stream;

        public AsyncEnumerable(System.IO.Stream stream)
        {
            this.stream = stream;
        }

        public IEnumerator<string> GetEnumerator()
        {
            return new AsyncEnumerator(stream);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return new AsyncEnumerator(stream);
        }
    }

    /// <summary>
    /// Common CloudFile readers.
    /// </summary>
    public static class CloudFile
    {
        /// <summary>
        /// Read file as a string.
        /// </summary>
        public static Func<System.IO.Stream, Task<string>> ReadAllText =
                async stream =>
                {
                    using (var sr = new StreamReader(stream))
                        return await sr.ReadToEndAsync();
                };

        /// <summary>
        /// Lazily read all lines.
        /// </summary>
        public static Func<System.IO.Stream, Task<IEnumerable<string>>> ReadLines =
                stream => Task.FromResult<IEnumerable<string>>(new AsyncEnumerable(stream));

        /// <summary>
        /// Read all lines.
        /// </summary>
        public static Func<System.IO.Stream, Task<string[]>> ReadAllLines =
                async stream =>
                {
                    var lines = new List<string>();
                    using (var sr = new StreamReader(stream))
                        lines.Add(await sr.ReadLineAsync());
                    return lines.ToArray();
                };

        /// <summary>
        /// Read all bytes.
        /// </summary>
        public static Func<System.IO.Stream, Task<byte[]>> ReadAllBytes =
                async stream =>
                {
                    using (var ms = new MemoryStream())
                    {
                        await stream.CopyToAsync(ms);
                        return ms.ToArray();
                    }
                };
    }

}
