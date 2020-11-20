using System;
using System.Threading;
using System.Threading.Tasks;

namespace ReproAmqp
{
  public static class Program
  {
    public static async Task Main()
    {
      var config = new Config
      {
        AddDelayAfterOpen = true,
        KeyName = "RootManageSharedAccessKey",
        Endpoint = "sb://setme.servicebus.windows.net",
        SharedAccessSignature = "setme"
      };

      Console.WriteLine("Press any key to stop the test...");
      Console.WriteLine();

      var runner = new TestRunner(config);

      var cts = new CancellationTokenSource();
      var running = runner.RunAsync(cts.Token);

      Console.ReadKey();
      Console.WriteLine();

      cts.Cancel();
      await running;
    }
  }
}
