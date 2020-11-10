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
      managementClient.StartSimulatingTransientErrors();

      var publisher = new AzureServiceBusMessagePublisher(config);

      while (!token.IsCancellationRequested)
      {
        var receiver = AzureServiceBusUtilities.CreateMessageReceiver(config);
        await Task.Delay(config.RecycleInterval);
        await receiver.CloseAsync();

        var link = AzureServiceBusUtilities.GetLinkFromReceiver(receiver);
        if (link.TryGetOpenedObject(out var _))
        {
          Console.WriteLine($"Received open link from receiver with clientId={receiver.ClientId}");
        }
      }

      await publisher.Shutdown();
      await managementClient.Shutdown();
    }
  }
}
