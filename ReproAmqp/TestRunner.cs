using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproAmqp
{
  public class Config
  {
    public bool AddDelayAfterOpen { get; set; }
    public string Endpoint { get; set; }
    public string KeyName { get; set; }
    public string SharedAccessSignature { get; set; }
  }

  public class TestRunner
  {
    private readonly Random _rand = new Random();
    private readonly TimeSpan _defaultTimeSpan = TimeSpan.FromSeconds(60);

    private readonly Config _config;

    private int TotalTestsCounter = 0;
    private int GotOpenConnectionCounter = 0;
    private int GotClosedConnectionCounter = 0;
    private int UnsuccessfulManagerCloseCounter = 0;

    public TestRunner(Config config)
    {
      _config = config;
    }

    public async Task RunAsync(CancellationToken token)
    {
      while (!token.IsCancellationRequested)
      {
        var connectionManager = SetupConnectionManager();

        var creating = connectionManager.GetOrCreateAsync(_defaultTimeSpan);
        
        // this is really only added to illustrate that the window for this error is decently large
        if (_config.AddDelayAfterOpen)
        {
          await Task.Delay(_rand.Next(100, 501));
        }

        await connectionManager.CloseAsync();

        try
        {
          var connection = await creating;

          bool managerClosed = false;
          try
          {
            await connectionManager.GetOrCreateAsync(_defaultTimeSpan);
          }
          catch (ObjectDisposedException)
          {
            managerClosed = true;
          }

          // give it a bit in case it is closing it in the background
          await Task.Delay(100);

          ++TotalTestsCounter;

          if (!managerClosed)
          {
            ++UnsuccessfulManagerCloseCounter;
          }
          else if (!connection.IsClosing())
          {
            Console.WriteLine("Got open connection from closed connectionManager!");
            ++GotOpenConnectionCounter;
            await connection.CloseAsync(_defaultTimeSpan);
          }
          else
          {
            ++GotClosedConnectionCounter;
          }
        }
        catch (Exception ex)
        {
          Console.WriteLine(ex);
        }
      }

      PrintStats();
    }

    private FaultTolerantAmqpObject<AmqpConnection> SetupConnectionManager()
    {
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(_config.KeyName, _config.SharedAccessSignature, TimeSpan.FromDays(1));

      var connection = new ServiceBusConnection(_config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      var prop = connection.GetType().GetProperty("ConnectionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      return prop.GetValue(connection) as FaultTolerantAmqpObject<AmqpConnection>;
    }

    private void PrintStats()
    {
      Console.WriteLine($"Overall stats: TotalTests={TotalTestsCounter} GotClosedConnection={GotClosedConnectionCounter} GotOpenConnection={GotOpenConnectionCounter} " +
        $"UnsuccessfulManagerCloseCounter={UnsuccessfulManagerCloseCounter}");
    }
  }
}
