using System;

namespace PostgreConnection.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class IgnorePropertyAttribute : Attribute
    {
        public IgnorePropertyAttribute()
        {
        }
    }
}
