using System;
using System.Threading;
using System.Threading.Tasks;

namespace ReproConsistent
{
  public static class AzureServiceBusLinkTest
  {
    public static async Task RunTestAsync(CancellationToken token, AzureServiceBusConfiguration config)
    {
      var managementClient = new AzureServiceBusManagementClientWrapper(config);
      await managementClient.IntializeEntities();
      //managementClient.StartSimulatingTransientErrors();

      var publisher = new AzureServiceBusMessagePublisher(config);
      
      Console.WriteLine("Starting test...");

      while (!token.IsCancellationRequested)
      {
        var receiver = new AzureServiceBusMessageReceiver(config);
        await Task.Delay(1000);

        var connectionManager = receiver.GetAmqpConnectionManager();
        if (!connectionManager.TryGetOpenedObject(out var connection))
        {
          throw new TimeoutException("whatever");
        }

        await connection.CloseAsync(TimeSpan.FromSeconds(1));

        var littleTimeout = new CancellationTokenSource(10000);
        await receiver.WaitForError(token, littleTimeout.Token);

        Console.WriteLine("Receiver found error or timed out looking for error, recycling");
        await receiver.Shutdown();

        var linkManager = receiver.GetLinkManager();
        if (linkManager.TryGetOpenedObject(out var _))
        {
          Console.WriteLine($"Received open link from receiver with clientId={receiver.ClientId}");
        }
      }

      await publisher.Shutdown();
      await managementClient.Shutdown();
    }
  }
}
