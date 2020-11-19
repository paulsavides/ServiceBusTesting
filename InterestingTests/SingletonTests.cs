using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Amqp;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace InterestingTests
{
  [TestClass]
  public class SingletonTests
  {
    [TestMethod]
    public async Task SingletonConcurrenctCloseOpenTests()
    {
      var createTcs = new TaskCompletionSource<object>();
      var closeTcs = new TaskCompletionSource<object>();

      var singleton = new SingletonTester(createTcs.Task, closeTcs.Task);

      var creating = singleton.GetOrCreateAsync(TimeSpan.FromDays(1000));
      var closing = singleton.CloseAsync();

      closeTcs.SetResult(new object());
      await closing;

      createTcs.SetResult(new object());
      await creating;

      var createdObj = GetInternalProperty<object>(singleton, "Value");
      try
      {
        await singleton.GetOrCreateAsync(TimeSpan.FromDays(1000));
        Assert.Fail("Singleton should throw an ObjectDisposedException");
      }
      catch (ObjectDisposedException) { }

      createdObj.Should().BeNull("A closed Singleton shouldn't have a value!");
    }

    private class SingletonTester : Singleton<object>
    {
      private readonly Task<object> _onCreateComplete;
      private readonly Task<object> _onSafeCloseComplete;

      public SingletonTester(Task<object> onCreateComplete, Task<object> onSafeCloseComplete)
      {
        _onCreateComplete = onCreateComplete;
        _onSafeCloseComplete = onSafeCloseComplete;
      }

      protected override async Task<object> OnCreateAsync(TimeSpan timeout)
      {
        await _onCreateComplete;
        return new object();
      }

      protected override void OnSafeClose(object value)
      {
        _onSafeCloseComplete.GetAwaiter().GetResult();
      }
    }

    private T GetInternalProperty<T>(object from, string propertyName)
      where T : class
    {
      var prop = from.GetType().GetProperty(propertyName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      return prop.GetValue(from) as T;
    }
  }
}
