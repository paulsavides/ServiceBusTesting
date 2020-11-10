using System;
using System.Threading;
using System.Threading.Tasks;

namespace ReproConsistent
{
  public static class AzureServiceBusLinkTest
  {
    public static async Task RunTestAsync(CancellationToken token, AzureServiceBusConfiguration config)
    {
      while (!token.IsCancellationRequested)
      {
        var receiver = AzureServiceBusUtilities.CreateMessageReceiver(config);
        await receiver.CloseAsync();

        var link = AzureServiceBusUtilities.GetLinkFromReceiver(receiver);
        if (link.TryGetOpenedObject(out var _))
        {
          Console.WriteLine($"Received open link from receiver with clientId={receiver.ClientId}");
        }
      }
    }
  }
}
