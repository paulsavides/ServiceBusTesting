using System;
using System.Collections.Generic;
using System.Text;

namespace ReproProject
{
  public static class Utilities
  {
    public static TProp GetInternalProperty<TFrom, TProp>(this TFrom from, string propertyName)
      where TProp : class
    {
      var property = from.GetType().GetProperty(propertyName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      return property.GetValue(from) as TProp;
    }
  }
}
