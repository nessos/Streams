using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.MBrace;
using Nessos.MBrace.Client;
using Microsoft.FSharp.Core;
using Microsoft.FSharp.Collections;
using MBraceStoreClient = Nessos.MBrace.Client.StoreClient;
using Nessos.Streams.Internals.Cloud;

namespace Nessos.Streams.Cloud.CSharp.MBrace
{
    /// <summary>
    /// MBraceNode wrapper class.
    /// </summary>
    public class Node
    {
        internal MBraceNode node;
        internal Node (MBraceNode node) { this.node = node; }

        /// <summary>
        /// Connect ton an existing node.
        /// </summary>
        /// <param name="uri"></param>
        /// <returns></returns>
        public Node Connect(string uri) 
        {
            return new Node(MBraceNode.Connect(uri));
        }

        /// <summary>
        /// Ping node and return roundtrip time.
        /// </summary>
        /// <returns></returns>
        public TimeSpan Ping()
        {
            return this.node.Ping(null);
        }
    }

    /// <summary>
    /// Static API wrapper.
    /// </summary>
    public static class MBrace
    {
        /// <summary>
        /// Runs in memory the given computation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="computation"></param>
        /// <returns></returns>
        public static T RunLocal<T>(Cloud<T> computation)
        {
            return Nessos.MBrace.Client.ClientExtensions.MBrace.RunLocal(computation, null);
        }
    }

    /// <summary>
    /// Process wrapper class.
    /// </summary>
    public class Process<T>
    {
        internal Nessos.MBrace.Client.Process<T> process;
        internal Process(Nessos.MBrace.Client.Process<T> p) { this.process = p; }

        /// <summary>
        /// Synchronously waits for the result.
        /// </summary>
        /// <returns></returns>
        public T AwaitResult()
        {
            return this.process.AwaitResult(null);
        }

        /// <summary>
        /// Kills cloud process.
        /// </summary>
        public void Kill()
        {
            this.process.Kill();
        }
    }

    /// <summary>
    /// StoreClient wrapper.
    /// </summary>
    public class StoreClient 
    {
        internal MBraceStoreClient client;
        internal StoreClient(MBraceStoreClient c)
        {
            this.client = c;
        }
        /// <summary>
        /// Gets the default StoreClient.
        /// </summary>
        public static StoreClient Default { get { return new StoreClient(MBraceStoreClient.Default); } }

        /// <summary>
        /// Upload given files as CloudFiles.
        /// </summary>
        /// <param name="paths"></param>
        /// <param name="container"></param>
        /// <returns></returns>
        public ICloudFile[] UploadFiles(string[] paths, string container)
        {
            return this.client.UploadFiles(paths, FSharpOption<string>.Some(container));
        }

        /// <summary>
        /// Get contained CloudFiles.
        /// </summary>
        /// <param name="container"></param>
        /// <returns></returns>
        public ICloudFile[] EnumerateCloudFiles(string container)
        {
            return this.client.EnumerateCloudFiles(container);
        }

        /// <summary>
        /// Create a new CloudArray.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="container"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public ICloudArray<TSource> CreateCloudArray<TSource>(string container, IEnumerable<TSource> values)
        {
            return this.client.CreateCloudArray<TSource>(container, values);
        }
    }

    /// <summary>
    /// MBraceSettings wrapper class.
    /// </summary>
    public static class Settings
    {
        /// <summary>
        /// Path to the mbrace daemon executable.
        /// </summary>
        public static string MBracedExecutablePath
        {
            get { return MBraceSettings.MBracedExecutablePath;  } 
            set { MBraceSettings.MBracedExecutablePath = value; }
        }

        /// <summary>
        /// Default ICloudStore implementation used by the client.
        /// </summary>
        public static Nessos.MBrace.Store.ICloudStore DefaultStore
        {
            get { return MBraceSettings.DefaultStore; }
            set { MBraceSettings.DefaultStore = value; }
        }
    }

    /// <summary>
    /// MBraceRuntime wrapper class.
    /// </summary>
    public class Runtime
    {
        MBraceRuntime runtime;

        /// <summary>
        /// Creates a new local runtime.
        /// </summary>
        /// <param name="totalNodes"></param>
        /// <returns></returns>
        public static Runtime InitLocal(int totalNodes)
        {
            return new Runtime(MBraceRuntime.InitLocal(totalNodes, null, null, null, null, null, null, null));
        }

        /// <summary>
        /// Connect to an existing runtime.
        /// </summary>
        /// <param name="uri"></param>
        /// <returns></returns>
        public static Runtime Connect(string uri)
        {
            return new Runtime(MBraceRuntime.Connect(uri));
        }

        /// <summary>
        /// Boots a new runtime.
        /// </summary>
        /// <param name="nodes"></param>
        /// <returns></returns>
        public static Runtime Boot(Node[] nodes)
        {
            var nodeList = nodes.Aggregate(FSharpList<MBraceNode>.Empty, (s,n) => FSharpList<MBraceNode>.Cons(n.node,s));
            return new Runtime(MBraceRuntime.Boot(nodeList, null, null, null));
        }

        internal Runtime(MBraceRuntime runtime)
        {
            this.runtime = runtime;
        }

        /// <summary>
        /// Runs the given computation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="computation"></param>
        /// <returns></returns>
        public T Run<T>(Cloud<T> computation)
        {
            return this.runtime.Run(computation, null, null);
        }

        /// <summary>
        /// Creates a new cloud process.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="computation"></param>
        /// <returns></returns>
        public Process<T> CreateProcess<T>(Cloud<T> computation)
        {
            return new Process<T>(this.runtime.CreateProcess<T>(computation, null));
        }

        /// <summary>
        /// Prints runtime information.
        /// </summary>
        /// <param name="showPerformanceCounters"></param>
        public void ShowInfo(bool showPerformanceCounters = false)
        {
            this.runtime.ShowInfo(FSharpOption<bool>.Some(showPerformanceCounters));
        }

        /// <summary>
        /// Prints cloud process information.
        /// </summary>
        public void ShowProcessInfo()
        {
            this.runtime.ShowProcessInfo();
        }

        /// <summary>
        /// Reboot runtime.
        /// </summary>
        public void Reboot()
        {
            this.runtime.Reboot(null, null, FSharpOption<Nessos.MBrace.Store.ICloudStore>.None);
        }

        /// <summary>
        /// Kill local runtime.
        /// </summary>
        public void Kill()
        {
            this.runtime.Kill();
        }
    }
}
