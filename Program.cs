using GridEx.API;
using GridEx.API.Requests;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace GridEx.HftClient
{
	class Program
	{
		const long TotalAmountOfOrdersForTest = 1000000000;
		const long StatisticsStepSize = 100000;
		const int AmountOfPublishers = 64;
		const int HftServerPort = 7777;

		static readonly Random _random = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
		static readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
		static readonly ManualResetEvent _manualResetEvent = new ManualResetEvent(false);
		static readonly Stopwatch _currentTPS = new Stopwatch();
		static readonly CountdownEvent _countdownEvent = new CountdownEvent(AmountOfPublishers);

		static long _cancelledOrders = 0;
		static long _createdOrders = 0;
		static long _executedOrders = 0;
		static long _completedOrders = 0;
		static long _rejectedOrders = 0;
		static long _lastCancelledOrders = 0;
		static long _rejectedRequests = 0;
		static long _processedOrders = 0;
		static long _processedOrdersForStep = 0;
		static int _currentTPSUpdating = 0;

		static void Main(string[] args)
		{
			PrintWelcome();

			if (args.Length != 1)
			{
				Console.WriteLine("HFT Server IP was not found.");
				PrintHelp();
				return;
			}

			var hftServerIP = args[0];
			IPEndPoint hftServerEndpoint = null;
			try
			{
				hftServerEndpoint = new IPEndPoint(IPAddress.Parse(args[0]).MapToIPv4(), HftServerPort);
			}
			catch (Exception)
			{
				Console.WriteLine($"Invalid HFT Server IP: {hftServerIP}.");
				return;
			}

			Console.WriteLine("Preparing tasks for stress test and connect to HFT Server...");
			var tasks = new List<Task>();

			foreach (var userId in Enumerable.Range(0, AmountOfPublishers).Select(_ => Guid.NewGuid()))
			{
				var hftSocket = new HftSocket();

				tasks.Add(
					Task.Factory.StartNew(
						() => RunHftSocket(hftSocket, hftServerEndpoint, userId),
						TaskCreationOptions.LongRunning));

				var batchOrdersSize = _random.Next(10, 20);

				tasks.Add(
					Task.Factory.StartNew(
						() => OrderRushLoop(hftSocket, batchOrdersSize, TotalAmountOfOrdersForTest / AmountOfPublishers),
						TaskCreationOptions.LongRunning));
			}

			_countdownEvent.Wait();
			Console.WriteLine("Done.");

			var commonTPS = Stopwatch.StartNew();

			_currentTPS.Start();
			_manualResetEvent.Set();

			Console.WriteLine($"Start stress test for {TotalAmountOfOrdersForTest} orders...");

			Task.WhenAll(tasks.ToArray());

			_cancellationTokenSource.Token.WaitHandle.WaitOne();

			commonTPS.Stop();

			Console.WriteLine("Stress test completed.");
			Console.WriteLine("=======================");
			Console.WriteLine("Throughput statistics:");

			Console.WriteLine($"Total TPS:{ _processedOrders / commonTPS.Elapsed.TotalSeconds}");
			Console.WriteLine($"Processed orders(include CANCEL ALL orders): {_processedOrders}.");
			Console.WriteLine($"Created BUY/SELL LIMIT orders: {_createdOrders}.");
			Console.WriteLine($"Executed BUY/SELL LIMIT orders: {_executedOrders}.");
			Console.WriteLine($"Completed BUY/SELL LIMIT orders: {_completedOrders}.");
			Console.WriteLine($"Cancelled BUY/SELL LIMIT orders: {_cancelledOrders}.");
			Console.WriteLine($"Rejected orders: {_rejectedOrders}.");
			Console.WriteLine($"Rejected requests: {_rejectedRequests}.");
		}

		private static void RunHftSocket(HftSocket hftSocket, IPEndPoint hftServerEndpoint, Guid userId)
		{
			hftSocket.OnDisconnected += socket =>
			{
				_cancellationTokenSource.Cancel();
			};

			hftSocket.OnConnected += socket =>
			{
			};

			hftSocket.OnError += (socket, error) =>
			{
				RunAsyncConsole(error.ToString());
			};

			hftSocket.OnException += (socket, exception) =>
			{
				RunAsyncConsole(exception.ToString());
				_cancellationTokenSource.Cancel();
			};

			hftSocket.OnRequestRejected += (socket, eventArgs) =>
			{
				Interlocked.Increment(ref _rejectedRequests);

				CalculateOrderProcessed(hftSocket, 1);
			};

			hftSocket.OnUserTokenAccepted += (socket, eventArgs) =>
			{
				_countdownEvent.Signal();
				RunAsyncConsole($"User token {eventArgs.Token} accepted.");
			};

			hftSocket.OnUserTokenRejected += (socket, eventArgs) =>
			{
				RunAsyncConsole("User token rejected.");
			};

			hftSocket.OnAllOrdersCancelled += (socket, eventArgs) =>
			{
				var cancelledOrders = Interlocked.Add(ref _cancelledOrders, eventArgs.Amount);
				if (cancelledOrders - _lastCancelledOrders >= StatisticsStepSize)
				{
					Interlocked.Add(ref _lastCancelledOrders, StatisticsStepSize);

					RunAsyncConsole($"Cancelled orders: {cancelledOrders}.");
				}

				CalculateOrderProcessed(hftSocket, eventArgs.Amount + 1);
			};

			hftSocket.OnOrderCancelled += (socket, eventArgs) =>
			{
				var cancelledOrders = Interlocked.Increment(ref _cancelledOrders);
				if (cancelledOrders - _lastCancelledOrders >= StatisticsStepSize)
				{
					Interlocked.Add(ref _lastCancelledOrders, StatisticsStepSize);

					RunAsyncConsole($"Cancelled orders: {cancelledOrders}.");
				}

				CalculateOrderProcessed(hftSocket, 1);
			};

			hftSocket.OnMarketInfo += (socket, eventArgs) =>
			{

			};

			hftSocket.OnOrderCreated += (socket, eventArgs) =>
			{
				var createdOrders = Interlocked.Increment(ref _createdOrders);
				if (createdOrders % StatisticsStepSize == 0)
				{
					RunAsyncConsole($"Created orders: {createdOrders}.");
				}
			};

			hftSocket.OnOrderRejected += (socket, eventArgs) =>
			{
				var rejectedOrders = Interlocked.Increment(ref _rejectedOrders);
				if (rejectedOrders % StatisticsStepSize == 0)
				{
					RunAsyncConsole($"Rejected orders: {rejectedOrders}.");
				}

				CalculateOrderProcessed(hftSocket, 1);
			};

			hftSocket.OnConnectionTooSlow += (socket, eventArgs) =>
			{
				RunAsyncConsole("Connection is too slow. Connection was closed.");
			};

			hftSocket.OnOrderExecuted += (socket, eventArgs) =>
			{
				var executedOrders = Interlocked.Increment(ref _executedOrders);
				if (executedOrders % StatisticsStepSize == 0)
				{
					RunAsyncConsole($"Executed orders: {executedOrders}.");
				}

				if (eventArgs.IsCompleted)
				{
					var completedOrders = Interlocked.Increment(ref _completedOrders);
					if (completedOrders % StatisticsStepSize == 0)
					{
						RunAsyncConsole($"Completed orders: {completedOrders}.");
					}

					CalculateOrderProcessed(hftSocket, 1);
				}
			};

			hftSocket.Connect(hftServerEndpoint);
			hftSocket.Send(new UserToken(0, userId.ToByteArray()));
			hftSocket.WaitResponses(_cancellationTokenSource.Token);
		}

		private static void CalculateOrderProcessed(HftSocket hftSocket, int orders)
		{
			var ordersProcessed = Interlocked.Add(ref _processedOrders, orders);
			var processedOrdersForStep = Interlocked.Add(ref _processedOrdersForStep, orders);

			if (processedOrdersForStep >= StatisticsStepSize)
			{
				_processedOrdersForStep = 0;

				if (Interlocked.CompareExchange(ref _currentTPSUpdating, 1, 0) == 0)
				{
					_currentTPS.Stop();
					var totalSeconds = _currentTPS.Elapsed.TotalSeconds;
					RunAsyncConsole($"Current TPS: {processedOrdersForStep / totalSeconds}.");
					_currentTPS.Reset();
					_currentTPS.Start();
					_currentTPSUpdating = 0;
				}
			}

			if (ordersProcessed >= TotalAmountOfOrdersForTest)
			{
				_cancellationTokenSource.Cancel();
			}
		}

		private static void OrderRushLoop(HftSocket socket, int batchSize, long amountOfOrders)
		{
			_manualResetEvent.WaitOne();

			var batchCounter = 0;
			var requestId = 0;
			int processedForUserOrders = 0;
			while (processedForUserOrders++ < amountOfOrders)
			{
				if (batchCounter < batchSize)
				{
					var orderType = processedForUserOrders % 2L == 0L ? RequestTypeCode.SellLimitOrder : RequestTypeCode.BuyLimitOrder;
					var price = _random.Next(10000000, 10020001) * 0.00000001;
					var volume = _random.Next(10000, 100001) * 0.000001;
					if (orderType == RequestTypeCode.BuyLimitOrder)
					{
						socket.Send(new BuyLimitOrder(requestId++, price, volume));
					}
					else
					{
						socket.Send(new SellLimitOrder(requestId++, price, volume));
					}

					++batchCounter;
				}
				else
				{
					socket.Send(new CancelAllOrders(requestId++));

					batchCounter = 0;
				}
			}

			socket.Send(new CancelAllOrders(requestId++));
		}

		private static void PrintWelcome()
		{
			Console.WriteLine("This is tool for stress testing of GridEx HFT Server.");
			Console.WriteLine("Performance depends by channel, ping to HFT Server and running stress test utils by other people.");
		}

		private static void PrintHelp()
		{
			Console.WriteLine("dotnet HftClient <IP>");
		}

		private static void RunAsyncConsole(string message)
		{
			Task.Run(() => Console.WriteLine(message));
		}
	}
}