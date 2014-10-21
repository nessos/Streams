using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.MBrace;
using Nessos.MBrace.Client;
using Microsoft.FSharp.Core;
using Microsoft.FSharp.Collections;
using Runtime = Nessos.MBrace.Client.MBraceRuntime;
using Node = Nessos.MBrace.Client.MBraceNode;
using Settings = Nessos.MBrace.Client.MBraceSettings;

namespace Streams.Cloud.CSharp.MBraceClient;
{
	/// <summary>
	/// MBraceNode wrapper class.
	/// </summary>
	public class MBraceNode
	{
		internal Node node;
		internal MBraceNode (Node node) { this.node = node; }

		public MBraceNode Connect(string uri) 
		{
			return new MBraceNode(Node.Connect(uri));
		}

		public TimeSpan Ping()
		{
			return this.node.Ping(null);
		}
	}

	/// <summary>
	/// Process wrapper class.
	/// </summary>
	public class MBraceProcess<T>
	{
		internal Process<T> process;
		internal MBraceProcess(Process<T> p) { this.process = p; }

		public T AwaitResult()
		{
			return this.process.AwaitResult(null);
		}

		public void Kill()
		{
			this.process.Kill();
		}
	}

	public static class MBraceSettings
	{
		public static string MBracedExecutablePath
		{
			get { return Settings.MBracedExecutablePath;  }
			set { Settings.MBracedExecutablePath = value; }
		}

		public static Nessos.MBrace.Store.ICloudStore DefaultStore
		{
			get { return Settings.DefaultStore; }
			set { Settings.DefaultStore = value; }
		}
	}

	/// <summary>
	/// MBraceRuntime wrapper class.
	/// </summary>
	public class MBraceRuntime
	{
		Runtime runtime;

		static MBraceRuntime InitLocal(int totalNodes)
		{
			return new MBraceRuntime(Runtime.InitLocal(totalNodes, null, null, null, null, null, null, null));
		}

		static MBraceRuntime Connect(string uri)
		{
			return new MBraceRuntime(Runtime.Connect(uri));
		}

		static MBraceRuntime Boot(MBraceNode [] nodes)
		{
			var nodeList = nodes.Aggregate(FSharpList<Node>.Empty, (s,n) => FSharpList<Node>.Cons(n.node,s));
			return new MBraceRuntime(Runtime.Boot(nodeList, null, null, null));
		}

		private MBraceRuntime(Runtime runtime)
		{
			this.runtime = runtime;
		}

		public T Run<T>(Cloud<T> computation)
		{
			return this.runtime.Run(computation, null, null);
		}

		public MBraceProcess<T> CreateProcess<T>(Cloud<T> computation)
		{
			return new MBraceProcess<T>(this.runtime.CreateProcess<T>(computation, null));
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
