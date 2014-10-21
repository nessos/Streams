using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.MBrace;
using Nessos.MBrace.Client;
using Microsoft.FSharp.Core;
using Microsoft.FSharp.Collections;

namespace Nessos.Streams.Cloud.CSharp.MBrace
{
	/// <summary>
	/// MBraceNode wrapper class.
	/// </summary>
	public class Node
	{
		internal MBraceNode node;
		internal Node (MBraceNode node) { this.node = node; }

		public Node Connect(string uri) 
		{
			return new Node(MBraceNode.Connect(uri));
		}

		public TimeSpan Ping()
		{
			return this.node.Ping(null);
		}
	}

	public static class MBrace
	{
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

		public T AwaitResult()
		{
			return this.process.AwaitResult(null);
		}

		public void Kill()
		{
			this.process.Kill();
		}
	}

    /// <summary>
    /// MBraceSettings wrapper class.
    /// </summary>
	public static class Settings
	{
		public static string MBracedExecutablePath
		{
			get { return MBraceSettings.MBracedExecutablePath;  }
            set { MBraceSettings.MBracedExecutablePath = value; }
		}

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

		public static Runtime InitLocal(int totalNodes)
		{
			return new Runtime(MBraceRuntime.InitLocal(totalNodes, null, null, null, null, null, null, null));
		}

		public static Runtime Connect(string uri)
		{
			return new Runtime(MBraceRuntime.Connect(uri));
		}

		public static Runtime Boot(Node[] nodes)
		{
			var nodeList = nodes.Aggregate(FSharpList<MBraceNode>.Empty, (s,n) => FSharpList<MBraceNode>.Cons(n.node,s));
			return new Runtime(MBraceRuntime.Boot(nodeList, null, null, null));
		}

		internal Runtime(MBraceRuntime runtime)
		{
			this.runtime = runtime;
		}

		public T Run<T>(Cloud<T> computation)
		{
			return this.runtime.Run(computation, null, null);
		}

		public Process<T> CreateProcess<T>(Cloud<T> computation)
		{
			return new Process<T>(this.runtime.CreateProcess<T>(computation, null));
		}

		public void ShowInfo(bool showPerformanceCounters = false)
		{
			this.runtime.ShowInfo(FSharpOption<bool>.Some(showPerformanceCounters));
		}

		public void Reboot()
		{
			this.runtime.Reboot(null, null, FSharpOption<Nessos.MBrace.Store.ICloudStore>.None);
		}
	}
}
