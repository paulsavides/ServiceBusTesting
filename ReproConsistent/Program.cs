using System;
using System.Threading;
using System.Threading.Tasks;

namespace ReproConsistent
{
  public static class Program
  {
    public static async Task Main()
    {
      var config = new AzureServiceBusConfiguration
      {
        QueueName = "repro.consistent.queue",
        TopicName = "repro.consistent.topic",
        MaxConcurrentCalls = 10,
        Endpoint = "sb://mybus.servicebus.windows.net",
        KeyName = "RootManageSharedAccessKey",
        SharedAccessSignature = "replaceme",
        PublishInterval = 100,
        RecycleInterval = 2000,
        TransientErrorInterval = 1000
      };

      var tokenSource = new CancellationTokenSource();
      var running = AzureServiceBusLinkTest.RunTestAsync(tokenSource.Token, config);

      Console.WriteLine("Press any key to shut down and exit...");
      Console.ReadKey();

      Console.WriteLine("Shutting down...");
      
      tokenSource.Cancel();
      await running;

      Console.WriteLine("Shut down!");
    }
  }
}
